//! Sealed-segment transfer over the replica plane (#270).
//!
//! The repair path for a follower that fell below the leader's retransmission
//! window: the reconnect catch-up in [`super::network`] can only replay what
//! its bounded buffer still holds, and a follower behind that window is
//! stranded without a way to receive history. This module ships the leader's
//! SEALED prefix — `.segment` + `.manifest.json` + `.producers`, verbatim —
//! over the same mTLS peer plane, into a [`SegmentReceiver`] that validates
//! and lands them. Follower ADOPTION of the received set is deliberately not
//! here; it is the next slice.
//!
//! [`LeaderSegmentTransferHandler`] is the leader-side server: it answers the
//! listing and bounded chunk reads from the broker's sealed prefix only, and
//! refuses everything else by name. [`SegmentTransferClient`] is the
//! receiving side's one-shot puller, shaped like
//! [`super::network::ReplicaStatusClient`]: connect, verify the peer's
//! certificate identity, pull, disconnect.

use super::network::{
    assert_peer_uuid, build_client_connector, peer_certs_client, ReplicaPeerHandler,
    ReplicaTlsMaterial, REPLICA_LIMITS,
};
use crate::{BrokerResult, LocalBroker, SealedSegmentHandle};
use std::io::{Read, Seek, SeekFrom};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_rustls::client::TlsStream as ClientTlsStream;
use tokio_rustls::TlsConnector;
use uuid::Uuid;
use vtop_log::{sealed_artifact_path, SegmentReceiver, StagedSegment, TransferArtifact};
use vtop_protocol::{
    read_frame, write_frame, ErrorCode, FetchSegmentChunkRequest, FetchSegmentChunkResponse,
    ListSealedSegmentsRequest, Message, RangeIdentity, SealedSegmentEntry, SegmentArtifact,
    WireFrame, MAX_SEGMENT_CHUNK_BYTES,
};

/// Serves a leader's sealed prefix to repairing peers.
///
/// Wraps the broker rather than the segment set because the refusals need
/// what only the broker knows: the range identity, the lease view for
/// fencing, and the ACTIVE tail's id — so a request for the tail is refused
/// by name instead of reported as an unknown segment.
pub struct LeaderSegmentTransferHandler {
    broker: Arc<LocalBroker>,
}

impl LeaderSegmentTransferHandler {
    pub fn new(broker: Arc<LocalBroker>) -> Self {
        Self { broker }
    }

    /// Epoch discipline, identical in shape to the append plane's
    /// `check_follower_fencing`: the request must carry exactly the granted
    /// epoch and the lease must be live. A transfer served under a stale
    /// epoch would repair a follower onto a deposed leader's history — the
    /// same split-brain write the epoch exists to prevent, delivered as
    /// files instead of appends.
    fn check_fencing(&self, request_epoch: u64) -> Result<(), (ErrorCode, String)> {
        let held = self.broker.held_fencing_epoch();
        if request_epoch != held {
            return Err((
                ErrorCode::Fenced,
                format!(
                    "transfer request carries fencing epoch {request_epoch}; this leader holds \
                     {held}"
                ),
            ));
        }
        let meta = self.broker.meta_fencing_epoch();
        if !meta.lease_active() || meta.get() != held {
            return Err((
                ErrorCode::Fenced,
                "this leader's lease is inactive or fenced by a newer metadata grant".to_owned(),
            ));
        }
        Ok(())
    }

    fn check_range(&self, range: &RangeIdentity) -> Result<(), (ErrorCode, String)> {
        if range != self.broker.range() {
            return Err((
                ErrorCode::WrongRange,
                "transfer request range identity does not match this leader".to_owned(),
            ));
        }
        Ok(())
    }

    fn sealed_handle(&self, segment_id: Uuid) -> Result<SealedSegmentHandle, (ErrorCode, String)> {
        if let Some(handle) = self
            .broker
            .sealed_segment_handles()
            .into_iter()
            .find(|handle| handle.segment_id == segment_id)
        {
            return Ok(handle);
        }
        // Two different refusals on purpose. The tail is a segment this
        // leader HAS — refusing it as "unknown" would read as corruption and
        // invite a retry that can never succeed. It is refused as what it is:
        // a request outside the transfer plane's contract.
        if segment_id == self.broker.active_segment_id() {
            return Err((
                ErrorCode::InvalidRequest,
                format!(
                    "segment {segment_id} is this range's active tail; only the sealed prefix \
                     is served, because tail bytes can be superseded by truncation while the \
                     transfer is in flight"
                ),
            ));
        }
        Err((
            ErrorCode::WrongLineage,
            format!("this leader holds no sealed segment {segment_id}"),
        ))
    }
}

/// Byte size of `path`, where absence is a NAMED size of zero only for the
/// frontier — any other artifact of a sealed segment must exist.
fn artifact_size(path: &std::path::Path, required: bool) -> Result<u64, (ErrorCode, String)> {
    match std::fs::metadata(path) {
        Ok(metadata) => Ok(metadata.len()),
        Err(_) if !required => Ok(0),
        Err(source) => Err((
            ErrorCode::Storage,
            format!("cannot stat sealed artifact {}: {source}", path.display()),
        )),
    }
}

impl ReplicaPeerHandler for LeaderSegmentTransferHandler {
    fn node_id(&self) -> Uuid {
        self.broker.node_id()
    }

    fn apply_append(
        &self,
        _request: &vtop_protocol::ReplicaAppendRequest,
    ) -> Result<vtop_protocol::ReplicaAppendResponse, (ErrorCode, String)> {
        Err((
            ErrorCode::InvalidRequest,
            "this peer is a range leader; it replicates out, it does not accept replica appends"
                .to_owned(),
        ))
    }

    fn apply_append_batch(
        &self,
        _requests: &[vtop_protocol::ReplicaAppendRequest],
    ) -> Result<vtop_protocol::ReplicaAppendResponse, (ErrorCode, String)> {
        Err((
            ErrorCode::InvalidRequest,
            "this peer is a range leader; it replicates out, it does not accept replica appends"
                .to_owned(),
        ))
    }

    fn observe_hwm(
        &self,
        _update: &vtop_protocol::CommittedHwmUpdate,
    ) -> Result<(), (ErrorCode, String)> {
        Err((
            ErrorCode::InvalidRequest,
            "this peer is a range leader; the committed high-water mark flows FROM it".to_owned(),
        ))
    }

    fn status(
        &self,
        range: &RangeIdentity,
    ) -> Result<vtop_protocol::ReplicaStatusResponse, (ErrorCode, String)> {
        self.check_range(range)?;
        let (local_committed_offset, next_offset) = self.broker.local_offsets();
        Ok(vtop_protocol::ReplicaStatusResponse {
            local_committed_offset,
            next_offset,
        })
    }

    fn list_sealed_segments(
        &self,
        range: &RangeIdentity,
        fencing_epoch: u64,
    ) -> Result<Vec<SealedSegmentEntry>, (ErrorCode, String)> {
        self.check_range(range)?;
        self.check_fencing(fencing_epoch)?;
        let storage_error = |problem: vtop_log::LogError| {
            (
                ErrorCode::Storage,
                format!("cannot resolve sealed artifact paths: {problem}"),
            )
        };
        self.broker
            .sealed_segment_handles()
            .into_iter()
            .map(|handle| {
                let manifest =
                    sealed_artifact_path(&handle.segment_path, TransferArtifact::Manifest)
                        .map_err(storage_error)?;
                let producers =
                    sealed_artifact_path(&handle.segment_path, TransferArtifact::Producers)
                        .map_err(storage_error)?;
                Ok(SealedSegmentEntry {
                    segment_id: handle.segment_id,
                    base_offset: handle.base_offset,
                    next_offset: handle.next_offset,
                    segment_bytes: artifact_size(&handle.segment_path, true)?,
                    manifest_bytes: artifact_size(&manifest, true)?,
                    // Zero means absent, and only the frontier may be: a
                    // range's first segment inherited nothing.
                    producers_bytes: artifact_size(&producers, false)?,
                })
            })
            .collect()
    }

    fn fetch_segment_chunk(
        &self,
        request: &FetchSegmentChunkRequest,
    ) -> Result<FetchSegmentChunkResponse, (ErrorCode, String)> {
        self.check_range(&request.range)?;
        self.check_fencing(request.fencing_epoch)?;
        let handle = self.sealed_handle(request.segment_id)?;
        let artifact = match request.artifact {
            SegmentArtifact::Segment => TransferArtifact::Segment,
            SegmentArtifact::Manifest => TransferArtifact::Manifest,
            SegmentArtifact::Producers => TransferArtifact::Producers,
        };
        let path = sealed_artifact_path(&handle.segment_path, artifact).map_err(|problem| {
            (
                ErrorCode::Storage,
                format!("cannot resolve sealed artifact path: {problem}"),
            )
        })?;
        if matches!(request.artifact, SegmentArtifact::Producers) && !path.exists() {
            return Err((
                ErrorCode::InvalidRequest,
                format!(
                    "sealed segment {} has no inherited producer frontier; the listing \
                     advertised zero bytes for it",
                    request.segment_id
                ),
            ));
        }
        let io = |source: std::io::Error| {
            (
                ErrorCode::Storage,
                format!("cannot read sealed artifact {}: {source}", path.display()),
            )
        };
        let mut file = std::fs::File::open(&path).map_err(io)?;
        let total_bytes = file.metadata().map_err(io)?.len();
        if request.offset > total_bytes {
            return Err((
                ErrorCode::InvalidRequest,
                format!(
                    "chunk offset {} is beyond the {total_bytes}-byte artifact",
                    request.offset
                ),
            ));
        }
        let length = u64::from(request.length).min(total_bytes - request.offset);
        file.seek(SeekFrom::Start(request.offset)).map_err(io)?;
        let mut bytes = vec![0_u8; length as usize];
        file.read_exact(&mut bytes).map_err(io)?;
        Ok(FetchSegmentChunkResponse { total_bytes, bytes })
    }
}

/// One-shot puller of a leader's sealed prefix into a [`SegmentReceiver`].
///
/// Shaped like [`super::network::ReplicaStatusClient`] rather than the
/// leader's persistent dialer, and for the same reason: a repair is an
/// operation with a beginning and an end, not a session, and reusing the
/// replication dialer would start a replication stream as a side effect of
/// running one.
pub struct SegmentTransferClient {
    connector: TlsConnector,
    /// Deadline PER ROUND TRIP, not per transfer: a sealed prefix can be
    /// arbitrarily large, and the failure this bounds is a peer that stops
    /// answering, which any single exchange detects.
    request_timeout: Duration,
}

impl SegmentTransferClient {
    pub fn new(material: ReplicaTlsMaterial) -> BrokerResult<Self> {
        Ok(Self {
            connector: build_client_connector(material)?,
            request_timeout: Duration::from_secs(5),
        })
    }

    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Pull every sealed segment `receiver` does not already hold.
    ///
    /// Returns the installed primary paths, oldest first. Idempotent across
    /// interruptions: a segment whose primary already landed is skipped (the
    /// primary renames last, so its presence proves the bundle verified), and
    /// a segment that was mid-receive left only ignorable staging debris the
    /// receiver swept at open.
    pub async fn transfer_sealed_prefix(
        &self,
        addr: SocketAddr,
        server_name: &str,
        expected_node: Uuid,
        range: &RangeIdentity,
        fencing_epoch: u64,
        receiver: &SegmentReceiver,
    ) -> BrokerResult<Vec<PathBuf>> {
        let name = rustls::pki_types::ServerName::try_from(server_name.to_owned())
            .map_err(|error| {
                crate::BrokerError::InvalidConfig(format!("server name {server_name:?}: {error}"))
            })?
            .to_owned();
        let mut stream = timeout(self.request_timeout, async {
            let tcp = TcpStream::connect(addr)
                .await
                .map_err(|source| crate::BrokerError::Io {
                    path: PathBuf::from("segment-transfer"),
                    source,
                })?;
            self.connector
                .connect(name, tcp)
                .await
                .map_err(|source| crate::BrokerError::Io {
                    path: PathBuf::from("segment-transfer-tls"),
                    source,
                })
        })
        .await
        .map_err(|_| crate::BrokerError::Timeout("segment transfer connect"))??;
        assert_peer_uuid(peer_certs_client(&stream), expected_node)?;

        let mut request_id = 1_u64;
        let listing = match self
            .round_trip(
                &mut stream,
                &mut request_id,
                Message::ListSealedSegmentsRequest(ListSealedSegmentsRequest {
                    range: range.clone(),
                    fencing_epoch,
                }),
            )
            .await?
        {
            Message::ListSealedSegmentsResponse(response) => response.segments,
            Message::Error(error) => {
                return Err(crate::BrokerError::InvalidConfig(format!(
                    "leader refused the sealed-segment listing: {:?} {}",
                    error.code, error.message
                )))
            }
            other => {
                return Err(crate::BrokerError::InvalidConfig(format!(
                    "unexpected reply to a sealed-segment listing: {other:?}"
                )))
            }
        };

        let receiver_error = |problem: vtop_log::LogError| {
            crate::BrokerError::InvalidConfig(format!("transfer receiver: {problem}"))
        };
        let mut installed = Vec::new();
        for entry in listing {
            if receiver
                .is_complete(entry.base_offset)
                .map_err(receiver_error)?
            {
                continue;
            }
            let mut staged = receiver.begin(entry.base_offset).map_err(receiver_error)?;
            for (wire, local, expected) in [
                (
                    SegmentArtifact::Segment,
                    TransferArtifact::Segment,
                    entry.segment_bytes,
                ),
                (
                    SegmentArtifact::Manifest,
                    TransferArtifact::Manifest,
                    entry.manifest_bytes,
                ),
                (
                    SegmentArtifact::Producers,
                    TransferArtifact::Producers,
                    entry.producers_bytes,
                ),
            ] {
                // Zero means the artifact does not exist (a first segment's
                // frontier), never "empty file": fetching it would be refused
                // by name on the other side.
                if expected == 0 {
                    continue;
                }
                self.fetch_artifact(
                    &mut stream,
                    &mut request_id,
                    range,
                    fencing_epoch,
                    &entry,
                    wire,
                    local,
                    expected,
                    &mut staged,
                )
                .await?;
                staged.finish_artifact(local).map_err(receiver_error)?;
            }
            installed.push(staged.install().map_err(receiver_error)?);
        }
        Ok(installed)
    }

    #[allow(clippy::too_many_arguments)]
    async fn fetch_artifact(
        &self,
        stream: &mut ClientTlsStream<TcpStream>,
        request_id: &mut u64,
        range: &RangeIdentity,
        fencing_epoch: u64,
        entry: &SealedSegmentEntry,
        wire: SegmentArtifact,
        local: TransferArtifact,
        expected_bytes: u64,
        staged: &mut StagedSegment,
    ) -> BrokerResult<()> {
        let receiver_error = |problem: vtop_log::LogError| {
            crate::BrokerError::InvalidConfig(format!("transfer receiver: {problem}"))
        };
        let mut offset = 0_u64;
        while offset < expected_bytes {
            let length = (expected_bytes - offset).min(u64::from(MAX_SEGMENT_CHUNK_BYTES)) as u32;
            let reply = self
                .round_trip(
                    stream,
                    request_id,
                    Message::FetchSegmentChunkRequest(FetchSegmentChunkRequest {
                        range: range.clone(),
                        fencing_epoch,
                        segment_id: entry.segment_id,
                        artifact: wire,
                        offset,
                        length,
                    }),
                )
                .await?;
            let chunk = match reply {
                Message::FetchSegmentChunkResponse(chunk) => chunk,
                Message::Error(error) => {
                    return Err(crate::BrokerError::InvalidConfig(format!(
                        "leader refused a segment chunk: {:?} {}",
                        error.code, error.message
                    )))
                }
                other => {
                    return Err(crate::BrokerError::InvalidConfig(format!(
                        "unexpected reply to a segment chunk request: {other:?}"
                    )))
                }
            };
            // Sealed artifacts are immutable, so the size the listing
            // advertised is a fact, not an estimate. A different total means
            // the directory is being rewritten under the leader — abandon the
            // copy rather than land a chimera of two histories.
            if chunk.total_bytes != expected_bytes {
                return Err(crate::BrokerError::InvalidConfig(format!(
                    "sealed artifact changed size mid-transfer: listed {expected_bytes} bytes, \
                     chunk reply claims {}",
                    chunk.total_bytes
                )));
            }
            if chunk.bytes.is_empty() || chunk.bytes.len() as u64 > u64::from(length) {
                return Err(crate::BrokerError::InvalidConfig(format!(
                    "segment chunk reply carried {} bytes for a {length}-byte request",
                    chunk.bytes.len()
                )));
            }
            staged
                .append_artifact(local, &chunk.bytes)
                .map_err(receiver_error)?;
            offset += chunk.bytes.len() as u64;
        }
        Ok(())
    }

    async fn round_trip(
        &self,
        stream: &mut ClientTlsStream<TcpStream>,
        request_id: &mut u64,
        message: Message,
    ) -> BrokerResult<Message> {
        let id = *request_id;
        *request_id += 1;
        timeout(self.request_timeout, async {
            write_frame(
                stream,
                &WireFrame {
                    request_id: id,
                    stream_id: 0,
                    message,
                },
                REPLICA_LIMITS,
            )
            .await?;
            let reply = read_frame(stream, REPLICA_LIMITS).await?.ok_or_else(|| {
                crate::BrokerError::InvalidConfig(
                    "leader closed the session mid-transfer".to_owned(),
                )
            })?;
            if reply.request_id != id {
                return Err(crate::BrokerError::InvalidConfig(format!(
                    "transfer reply answered request {} while {id} was pending",
                    reply.request_id
                )));
            }
            Ok(reply.message)
        })
        .await
        .map_err(|_| crate::BrokerError::Timeout("segment transfer round trip"))?
    }
}
