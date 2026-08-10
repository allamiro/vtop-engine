//! Sealed-segment transfer over the real mTLS replica plane (#270).
//!
//! A leader rolls its range a few times by producing through the ordinary
//! broker path, serves its sealed prefix through a [`ReplicaPeerServer`], and
//! a [`SegmentTransferClient`] lands the segments in an empty directory
//! through a [`SegmentReceiver`]. The assertions are the slice's contract:
//! byte-identity of the shipped artifacts, receiver-rebuilt sidecars that
//! verify, a received directory discovery accepts with zero quarantines, and
//! a torn transfer that leaves the directory clean and resumable.

use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair};

/// The identity a direct handler call presents.
///
/// The handler itself serves whoever reaches it — authorization lives on the
/// node that installs it — so this only has to be a stable UUID, not a
/// permitted one.
const TEST_PEER: Uuid = Uuid::from_u128(0xF00D);
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use std::fs;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use uuid::Uuid;
use vtop_broker::replication::{
    LeaderSegmentTransferHandler, ReplicaPeerHandler, ReplicaPeerServer, ReplicaTlsMaterial,
    SegmentTransferClient,
};
use vtop_broker::{LocalBroker, ProducerEpochJournal};
use vtop_log::env::Env;
use vtop_log::{
    sealed_artifact_path, segment_stem, KeyRange, RangeLineage, SegmentConfig, SegmentDescriptor,
    SegmentReader, SegmentReceiver, SegmentSet, StartupCatalog, TransferArtifact,
};
use vtop_protocol::{
    Durability as WireDurability, ErrorCode, FetchSegmentChunkRequest, FetchSegmentChunkResponse,
    Message, ProduceRecord, ProduceRequest, RangeIdentity, Role, SealedSegmentEntry,
    SegmentArtifact, WireFrame,
};

const LEADER: Uuid = Uuid::from_u128(0xA1);
const REPAIRER: Uuid = Uuid::from_u128(0xA9);
const PRODUCER: Uuid = Uuid::from_u128(0xB1);
const FENCING_EPOCH: u64 = 18;

struct CertBundle {
    cert: CertificateDer<'static>,
    key: PrivateKeyDer<'static>,
}

fn cert_for_cn(cn: &str) -> CertBundle {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
    params.distinguished_name = DistinguishedName::new();
    params.distinguished_name.push(DnType::CommonName, cn);
    let cert = params.self_signed(&key).unwrap();
    CertBundle {
        cert: cert.der().clone(),
        key: PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
    }
}

fn clone_private_key(key: &PrivateKeyDer<'static>) -> PrivateKeyDer<'static> {
    match key {
        PrivateKeyDer::Pkcs8(key) => {
            PrivatePkcs8KeyDer::from(key.secret_pkcs8_der().to_vec()).into()
        }
        _ => panic!("test keys are PKCS#8"),
    }
}

fn material(identity: &CertBundle, peer_trust: &[&CertBundle]) -> ReplicaTlsMaterial {
    let mut trust_roots = rustls::RootCertStore::empty();
    for peer in peer_trust {
        trust_roots.add(peer.cert.clone()).unwrap();
    }
    ReplicaTlsMaterial {
        certificate_chain: vec![identity.cert.clone()],
        private_key: clone_private_key(&identity.key),
        trust_roots,
    }
}

fn range_identity() -> RangeIdentity {
    RangeIdentity {
        topic: "events.v1".to_owned(),
        topic_epoch: 1,
        range_id: Uuid::from_u128(0xC1),
        range_generation: 0,
    }
}

/// A leader whose range has rolled a few times through the ordinary produce
/// path, so the sealed prefix carries real `.producers` frontiers.
fn rolled_leader(dir: &TempDir, range: &RangeIdentity) -> Arc<LocalBroker> {
    let descriptor = SegmentDescriptor {
        segment_id: Uuid::from_u128(0xD1),
        topic: range.topic.clone(),
        topic_epoch: range.topic_epoch,
        lineage: RangeLineage {
            range_id: range.range_id,
            generation: range.range_generation,
            key_range: KeyRange::full(),
            parents: Vec::new(),
        },
        base_offset: 0,
    };
    let config = SegmentConfig {
        max_record_bytes: 4096,
        max_group_bytes: 16 * 1024,
        max_segment_bytes: 16 * 1024,
        max_segment_records: 1000,
        index_stride: 2,
    };
    let segment = SegmentSet::create_in(&Env::real(), dir.path(), descriptor, config).unwrap();
    let epochs = ProducerEpochJournal::open(dir.path().join("epochs")).unwrap();
    let broker = LocalBroker::new(segment, epochs, range.clone(), FENCING_EPOCH).unwrap();
    for sequence in 0..48_u64 {
        let response = broker.handle(
            Role::Producer,
            WireFrame {
                request_id: sequence + 1,
                stream_id: 1,
                message: Message::ProduceRequest(ProduceRequest {
                    range: range.clone(),
                    fencing_epoch: FENCING_EPOCH,
                    producer_id: PRODUCER,
                    producer_epoch: 1,
                    first_sequence: sequence,
                    durability: WireDurability::LocalFsync,
                    records: vec![ProduceRecord {
                        timestamp_millis: 1_000 + sequence as i64,
                        key: b"k".to_vec(),
                        value: vec![b'x'; 900],
                    }],
                }),
            },
        );
        match response.message {
            Message::ProduceResponse(_) => {}
            other => panic!("produce {sequence} failed: {other:?}"),
        }
    }
    let broker = Arc::new(broker);
    assert!(
        broker.sealed_segment_handles().len() >= 2,
        "the fixture must roll at least twice, got {}",
        broker.sealed_segment_handles().len()
    );
    broker
}

fn spawn_leader_server(
    runtime: &Runtime,
    material: ReplicaTlsMaterial,
    handler: Arc<dyn ReplicaPeerHandler>,
) -> (SocketAddr, tokio::task::AbortHandle) {
    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = ReplicaPeerServer::new(material, LEADER, handler).unwrap();
        let handle = tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });
        (addr, handle.abort_handle())
    })
}

struct Harness {
    _leader_dir: TempDir,
    destination: TempDir,
    runtime: Runtime,
    _server_abort: tokio::task::AbortHandle,
    range: RangeIdentity,
    leader: Arc<LocalBroker>,
    addr: SocketAddr,
    client: SegmentTransferClient,
}

fn harness_with(
    wrap: impl FnOnce(LeaderSegmentTransferHandler) -> Arc<dyn ReplicaPeerHandler>,
) -> Harness {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let range = range_identity();
    let leader_cert = cert_for_cn(&LEADER.to_string());
    let repairer_cert = cert_for_cn(&REPAIRER.to_string());
    let leader_dir = tempfile::tempdir().unwrap();
    let leader = rolled_leader(&leader_dir, &range);
    let handler = wrap(LeaderSegmentTransferHandler::new(Arc::clone(&leader)));
    let (addr, abort) =
        spawn_leader_server(&runtime, material(&leader_cert, &[&repairer_cert]), handler);
    let client = SegmentTransferClient::new(material(&repairer_cert, &[&leader_cert])).unwrap();
    Harness {
        _leader_dir: leader_dir,
        destination: tempfile::tempdir().unwrap(),
        runtime,
        _server_abort: abort,
        range,
        leader,
        addr,
        client,
    }
}

fn harness() -> Harness {
    harness_with(|handler| Arc::new(handler) as Arc<dyn ReplicaPeerHandler>)
}

fn transfer(h: &Harness, receiver: &SegmentReceiver) -> Result<Vec<std::path::PathBuf>, String> {
    h.runtime
        .block_on(h.client.transfer_sealed_prefix(
            h.addr,
            "localhost",
            LEADER,
            &h.range,
            FENCING_EPOCH,
            receiver,
        ))
        .map_err(|error| error.to_string())
}

#[test]
fn sealed_prefix_transfers_byte_identically_and_discovers_clean() {
    let h = harness();
    let receiver = SegmentReceiver::open(&Env::real(), h.destination.path()).unwrap();
    let installed = transfer(&h, &receiver).unwrap();

    let handles = h.leader.sealed_segment_handles();
    assert_eq!(installed.len(), handles.len());

    for handle in &handles {
        // The three shipped artifacts are byte-identical, proven by hashing
        // both sides: byte identity IS the correctness mechanism, because v1
        // folds producer identity into derived ids that a decode-and-reappend
        // path would silently re-derive.
        for artifact in [
            TransferArtifact::Segment,
            TransferArtifact::Manifest,
            TransferArtifact::Producers,
        ] {
            let source = sealed_artifact_path(&handle.segment_path, artifact).unwrap();
            let name = source.file_name().unwrap();
            let received = h.destination.path().join(name);
            if !source.exists() {
                assert!(
                    !received.exists(),
                    "{name:?} must not be invented receiver-side"
                );
                continue;
            }
            assert_eq!(
                blake3::hash(&fs::read(&source).unwrap()),
                blake3::hash(&fs::read(&received).unwrap()),
                "{name:?} must ship verbatim"
            );
        }
        // `.index` (and `.chunks` for v2) never crossed the wire; the
        // receiver rebuilt them, and the reader open re-validates everything
        // against the manifest.
        let stem = segment_stem(handle.base_offset);
        assert!(h.destination.path().join(format!("{stem}.index")).exists());
        let reader =
            SegmentReader::open(h.destination.path().join(format!("{stem}.segment"))).unwrap();
        assert_eq!(reader.segment_id(), handle.segment_id);
        assert_eq!(reader.next_offset(), handle.next_offset);
    }

    let catalog = StartupCatalog::discover(h.destination.path()).unwrap();
    assert!(catalog.quarantined.is_empty(), "{:?}", catalog.quarantined);
    assert_eq!(catalog.entries.len(), handles.len());

    // Idempotent: a second run finds every segment already landed.
    let again = transfer(&h, &receiver).unwrap();
    assert!(again.is_empty(), "{again:?}");
}

#[test]
fn the_leader_refuses_by_name_what_the_transfer_plane_does_not_serve() {
    let h = harness();
    let handler = LeaderSegmentTransferHandler::new(Arc::clone(&h.leader));
    let fetch = |segment_id: Uuid, epoch: u64, range: RangeIdentity| FetchSegmentChunkRequest {
        range,
        fencing_epoch: epoch,
        segment_id,
        artifact: SegmentArtifact::Segment,
        offset: 0,
        length: 1024,
    };

    // Unknown segment: named as absent lineage, not as a bad request.
    let (code, message) = handler
        .fetch_segment_chunk(
            TEST_PEER,
            &fetch(Uuid::from_u128(0xDEAD), FENCING_EPOCH, h.range.clone()),
        )
        .unwrap_err();
    assert_eq!(code, ErrorCode::WrongLineage);
    assert!(message.contains("no sealed segment"), "{message}");

    // The active tail: a segment the leader HAS, refused as what it is.
    let (code, message) = handler
        .fetch_segment_chunk(
            TEST_PEER,
            &fetch(h.leader.active_segment_id(), FENCING_EPOCH, h.range.clone()),
        )
        .unwrap_err();
    assert_eq!(code, ErrorCode::InvalidRequest);
    assert!(message.contains("active tail"), "{message}");

    // Wrong range identity.
    let mut wrong_range = h.range.clone();
    wrong_range.range_id = Uuid::from_u128(0xFFF);
    let sealed = h.leader.sealed_segment_handles();
    let (code, _) = handler
        .fetch_segment_chunk(
            TEST_PEER,
            &fetch(sealed[0].segment_id, FENCING_EPOCH, wrong_range),
        )
        .unwrap_err();
    assert_eq!(code, ErrorCode::WrongRange);

    // Stale epoch, and the same refusal on the listing.
    let (code, _) = handler
        .fetch_segment_chunk(
            TEST_PEER,
            &fetch(sealed[0].segment_id, FENCING_EPOCH - 1, h.range.clone()),
        )
        .unwrap_err();
    assert_eq!(code, ErrorCode::Fenced);
    let (code, _) = handler
        .list_sealed_segments(TEST_PEER, &h.range, FENCING_EPOCH - 1)
        .unwrap_err();
    assert_eq!(code, ErrorCode::Fenced);

    // A frontier the listing advertised as absent cannot be fetched: the
    // first sealed segment inherited nothing.
    let first = sealed
        .iter()
        .min_by_key(|handle| handle.base_offset)
        .unwrap();
    let (code, message) = handler
        .fetch_segment_chunk(
            TEST_PEER,
            &FetchSegmentChunkRequest {
                artifact: SegmentArtifact::Producers,
                ..fetch(first.segment_id, FENCING_EPOCH, h.range.clone())
            },
        )
        .unwrap_err();
    assert_eq!(code, ErrorCode::InvalidRequest);
    assert!(
        message.contains("no inherited producer frontier"),
        "{message}"
    );

    // And over the wire, a deposed leader's refusal reaches the client as an
    // error, not as bytes.
    let stale = h.runtime.block_on(h.client.transfer_sealed_prefix(
        h.addr,
        "localhost",
        LEADER,
        &h.range,
        FENCING_EPOCH - 1,
        &SegmentReceiver::open(&Env::real(), h.destination.path()).unwrap(),
    ));
    let error = stale.unwrap_err().to_string();
    assert!(error.contains("Fenced"), "{error}");
}

/// Fails every chunk fetch after the first `allowed`, so a transfer dies
/// mid-artifact ON THE WIRE — the receiver has staged partial bytes when the
/// failure lands.
struct FailingLeader {
    inner: LeaderSegmentTransferHandler,
    allowed: AtomicUsize,
}

impl ReplicaPeerHandler for FailingLeader {
    fn node_id(&self) -> Uuid {
        self.inner.node_id()
    }
    fn apply_append(
        &self,
        request: &vtop_protocol::ReplicaAppendRequest,
    ) -> Result<vtop_protocol::ReplicaAppendResponse, (ErrorCode, String)> {
        self.inner.apply_append(request)
    }
    fn apply_append_batch(
        &self,
        requests: &[vtop_protocol::ReplicaAppendRequest],
    ) -> Result<vtop_protocol::ReplicaAppendResponse, (ErrorCode, String)> {
        self.inner.apply_append_batch(requests)
    }
    fn observe_hwm(
        &self,
        update: &vtop_protocol::CommittedHwmUpdate,
    ) -> Result<(), (ErrorCode, String)> {
        self.inner.observe_hwm(update)
    }
    fn status(
        &self,
        range: &RangeIdentity,
    ) -> Result<vtop_protocol::ReplicaStatusResponse, (ErrorCode, String)> {
        self.inner.status(range)
    }
    fn list_sealed_segments(
        &self,
        peer: Uuid,
        range: &RangeIdentity,
        fencing_epoch: u64,
    ) -> Result<Vec<SealedSegmentEntry>, (ErrorCode, String)> {
        self.inner.list_sealed_segments(peer, range, fencing_epoch)
    }
    fn fetch_segment_chunk(
        &self,
        peer: Uuid,
        request: &FetchSegmentChunkRequest,
    ) -> Result<FetchSegmentChunkResponse, (ErrorCode, String)> {
        if self.allowed.fetch_sub(1, Ordering::SeqCst) == 0 {
            self.allowed.store(0, Ordering::SeqCst);
            return Err((ErrorCode::Storage, "injected transfer fault".to_owned()));
        }
        self.inner.fetch_segment_chunk(peer, request)
    }
}

#[test]
fn a_transfer_torn_mid_artifact_leaves_the_directory_clean_and_resumable() {
    // Chunks are 1 MiB and the fixture's artifacts are tiny, so one allowed
    // fetch means the failure lands after the first artifact's first chunk:
    // mid-segment, with bytes already staged.
    let h = harness_with(|inner| {
        Arc::new(FailingLeader {
            inner,
            allowed: AtomicUsize::new(1),
        }) as Arc<dyn ReplicaPeerHandler>
    });
    let receiver = SegmentReceiver::open(&Env::real(), h.destination.path()).unwrap();
    let error = transfer(&h, &receiver).unwrap_err();
    assert!(error.contains("injected transfer fault"), "{error}");

    // The kill left only staging debris discovery ignores: zero entries,
    // zero quarantines. A half-received segment never looks like a real one.
    let catalog = StartupCatalog::discover(h.destination.path()).unwrap();
    assert!(catalog.quarantined.is_empty(), "{:?}", catalog.quarantined);
    assert!(catalog.entries.is_empty(), "{:?}", catalog.entries);

    // A retry against the still-broken leader keeps failing — worth pinning
    // on its own: a broken leader cannot dirty the directory no matter how
    // often the transfer retries, because the receiver sweeps its own debris
    // at open and stages everything under ignorable names.
    let receiver = SegmentReceiver::open(&Env::real(), h.destination.path()).unwrap();
    assert!(transfer(&h, &receiver).is_err());
    let catalog = StartupCatalog::discover(h.destination.path()).unwrap();
    assert!(catalog.quarantined.is_empty(), "{:?}", catalog.quarantined);

    // A healthy leader then completes the SAME torn directory.
    let repaired = harness();
    let receiver = SegmentReceiver::open(&Env::real(), h.destination.path()).unwrap();
    let installed = repaired
        .runtime
        .block_on(repaired.client.transfer_sealed_prefix(
            repaired.addr,
            "localhost",
            LEADER,
            &repaired.range,
            FENCING_EPOCH,
            &receiver,
        ))
        .unwrap();
    assert_eq!(
        installed.len(),
        repaired.leader.sealed_segment_handles().len()
    );
    let catalog = StartupCatalog::discover(h.destination.path()).unwrap();
    assert!(catalog.quarantined.is_empty(), "{:?}", catalog.quarantined);
    assert_eq!(catalog.entries.len(), installed.len());
}

/// A directory already holding a FOREIGN segment at the same base offset is
/// repaired, not stranded.
///
/// This is the case the mechanism exists for — a replica whose data directory
/// holds a previous incarnation of the range, or a segment torn by an
/// interrupted run — and it was the one case that could not be repaired. The
/// resume check reports a mismatched or unreadable primary as "not here, so
/// fetch it", while `install` refuses to replace a sealed segment, because
/// sealed means immutable. Each half is right and together they deadlocked:
/// the transfer would stage the whole prefix and then abort on publication,
/// every time, forever.
///
/// Replacement is now an explicit discard, and it is only safe because the
/// receiver owns the directory and every byte in it is re-fetchable.
#[test]
fn a_foreign_segment_at_the_same_offset_is_replaced_rather_than_stranding_the_repair() {
    let h = harness();
    let handles = h.leader.sealed_segment_handles();
    let victim = &handles[0];
    let stem = segment_stem(victim.base_offset);

    // Two shapes of "wrong bundle occupying the name", run through the same
    // path: one that opens and disagrees about identity, and one that does not
    // open at all.
    for (label, opens, plant) in [
        (
            "a segment from a previous incarnation of the range",
            true,
            Box::new(|dir: &std::path::Path, stem: &str| {
                // A real, valid, self-consistent segment — with a different
                // segment_id. It opens fine; only its identity is wrong.
                let foreign = TempDir::new().unwrap();
                let mut set = SegmentSet::create_in(
                    &Env::real(),
                    foreign.path(),
                    SegmentDescriptor {
                        segment_id: Uuid::from_u128(0xDEAD),
                        topic: range_identity().topic,
                        topic_epoch: range_identity().topic_epoch,
                        lineage: RangeLineage {
                            range_id: range_identity().range_id,
                            generation: range_identity().range_generation,
                            key_range: KeyRange::full(),
                            parents: Vec::new(),
                        },
                        base_offset: 0,
                    },
                    SegmentConfig {
                        max_segment_records: 4,
                        ..SegmentConfig::default()
                    },
                )
                .unwrap();
                for sequence in 0..12 {
                    set.append_group_minting(
                        &[vtop_log::LogRecord {
                            producer_id: PRODUCER,
                            producer_epoch: 0,
                            sequence,
                            timestamp_millis: 1_700_000_000_000,
                            attributes: 0,
                            key: b"k".to_vec(),
                            value: b"v".to_vec(),
                        }],
                        vtop_log::Durability::Fsync,
                    )
                    .unwrap();
                }
                let source = foreign.path().join(format!("{}.segment", segment_stem(0)));
                assert!(source.exists(), "the fixture must have rolled");
                for suffix in ["segment", "manifest.json", "index"] {
                    let from = source.with_extension("").with_extension(suffix);
                    let from = if suffix == "manifest.json" {
                        foreign
                            .path()
                            .join(format!("{}.manifest.json", segment_stem(0)))
                    } else {
                        from
                    };
                    if from.exists() {
                        std::fs::copy(&from, dir.join(format!("{stem}.{suffix}"))).unwrap();
                    }
                }
            }) as Box<dyn Fn(&std::path::Path, &str)>,
        ),
        (
            "a torn primary that does not open at all",
            false,
            Box::new(|dir: &std::path::Path, stem: &str| {
                std::fs::write(dir.join(format!("{stem}.segment")), b"not a segment").unwrap();
                std::fs::write(dir.join(format!("{stem}.manifest.json")), b"{}").unwrap();
            }) as Box<dyn Fn(&std::path::Path, &str)>,
        ),
    ] {
        let destination = TempDir::new().unwrap();
        plant(destination.path(), &stem);
        assert!(
            destination.path().join(format!("{stem}.segment")).exists(),
            "{label}: the fixture must actually occupy the name"
        );
        // Pin WHICH branch of the presence check each case exercises. Without
        // this both fixtures could quietly be the same "does not open" case,
        // and the identity comparison — the reason `presence` takes a
        // segment_id at all — would go untested.
        let planted = SegmentReader::open(destination.path().join(format!("{stem}.segment")));
        assert_eq!(
            planted.is_ok(),
            opens,
            "{label}: fixture must exercise the intended branch"
        );
        if let Ok(planted) = planted {
            assert_ne!(
                planted.segment_id(),
                victim.segment_id,
                "{label}: a foreign segment must differ in IDENTITY, not just in bytes"
            );
        }

        let receiver = SegmentReceiver::open(&Env::real(), destination.path()).unwrap();
        let installed = h
            .runtime
            .block_on(h.client.transfer_sealed_prefix(
                h.addr,
                "localhost",
                LEADER,
                &h.range,
                FENCING_EPOCH,
                &receiver,
            ))
            .unwrap_or_else(|error| panic!("{label}: repair must not be stranded: {error}"));

        // The whole prefix landed, including the offset that was occupied.
        assert_eq!(installed.len(), handles.len(), "{label}");
        let received =
            SegmentReader::open(destination.path().join(format!("{stem}.segment"))).unwrap();
        assert_eq!(
            received.segment_id(),
            victim.segment_id,
            "{label}: the leader's segment must have replaced the foreign one"
        );
        let catalog = StartupCatalog::discover(destination.path()).unwrap();
        assert!(
            catalog.quarantined.is_empty(),
            "{label}: {:?}",
            catalog.quarantined
        );
        assert_eq!(catalog.entries.len(), handles.len(), "{label}");

        // And a second run over the now-correct directory is a no-op: every
        // segment is Matching, so nothing is re-fetched or discarded.
        let receiver = SegmentReceiver::open(&Env::real(), destination.path()).unwrap();
        let again = transfer(&h, &receiver).unwrap();
        assert!(
            again.is_empty(),
            "{label}: a matching directory must transfer nothing, got {again:?}"
        );
    }
}

/// THE CLAIM OF #301, end to end: a transferred prefix becomes a range that
/// serves.
///
/// Transfer alone is not repair. A received prefix is bytes `SegmentSet::open_in`
/// refuses to open — its tail was sealed without a successor — so a replica
/// pointed at one would fail to start with an error about a missing active
/// segment, having successfully downloaded everything it needed. Repair is
/// transfer AND adoption, which is why `vtopctl node repair` does both in one
/// invocation rather than leaving the second to the operator.
///
/// The assertion is that every record the leader sealed is readable from the
/// rebuilt directory, and that the range accepts an append afterwards — a
/// replica that could be read but not written to would still be dead.
#[test]
fn a_transferred_prefix_becomes_a_range_that_serves_and_accepts_appends() {
    let h = harness();
    let receiver = SegmentReceiver::open(&Env::real(), h.destination.path()).unwrap();
    let installed = transfer(&h, &receiver).unwrap();
    assert!(!installed.is_empty(), "the fixture must transfer something");

    let handles = h.leader.sealed_segment_handles();
    let prefix_end = handles.last().expect("sealed prefix").next_offset;

    // Precondition: this is the refusal repair exists to resolve. Without it
    // the assertions below would prove nothing about adoption.
    let refused = SegmentSet::open_in(&Env::real(), h.destination.path())
        .map(|_| ())
        .expect_err("a prefix without a tail must not open as a range");
    assert!(
        refused.to_string().contains("no active segment"),
        "{refused}"
    );

    let mut repaired = SegmentSet::adopt_in(
        &Env::real(),
        h.destination.path(),
        Uuid::from_u128(0xC0FFEE),
    )
    .expect("the transferred prefix must adopt into a servable range");
    assert_eq!(
        repaired.next_offset(),
        prefix_end,
        "the tail must begin where the leader's sealed prefix ended"
    );

    // Every record the leader sealed, readable in one call from the rebuilt
    // directory — which is what "repaired" has to mean.
    let batch = repaired
        .fetch_through(0, 1 << 20, 10_000, prefix_end)
        .expect("a repaired range must serve reads");
    assert_eq!(
        batch.records.len() as u64,
        prefix_end,
        "every sealed record must be readable after repair"
    );
    for (index, record) in batch.records.iter().enumerate() {
        assert_eq!(record.offset, index as u64, "offsets must be contiguous");
    }

    // And writable: a replica that reads but cannot accept an append has not
    // rejoined anything.
    repaired
        .append_group_minting(
            &[vtop_log::LogRecord {
                producer_id: Uuid::from_u128(0xD00D),
                producer_epoch: 0,
                sequence: 0,
                timestamp_millis: 1_700_000_000_000,
                attributes: 0,
                key: b"post-repair".to_vec(),
                value: b"v".to_vec(),
            }],
            vtop_log::Durability::Fsync,
        )
        .expect("a repaired range must accept appends");
    assert_eq!(repaired.next_offset(), prefix_end + 1);
}

/// #315 — the lineage travels with the records. A transferred prefix plus an
/// installed epoch history can answer the reconciliation a leader transition
/// asks of it: the comparison against the source's history AGREES — not
/// "unknown", and not the divergence-at-zero that used to erase the repair —
/// and the journal accepts the next epoch adoption at the adopted tail.
#[test]
fn an_installed_epoch_history_lets_a_repaired_prefix_prove_its_lineage() {
    use vtop_broker::fencing_epochs::{
        install_transferred_history, EpochStart, FencingEpochJournal, Lineage,
    };

    let h = harness();
    let receiver = SegmentReceiver::open(&Env::real(), h.destination.path()).unwrap();
    let installed = transfer(&h, &receiver).unwrap();
    assert!(!installed.is_empty(), "the fixture must transfer something");
    let prefix_end = h
        .leader
        .sealed_segment_handles()
        .last()
        .expect("sealed prefix")
        .next_offset;

    // The source's lineage: one epoch for the older records, another for the
    // newer, and a third that begins exactly at the sealed end — which the
    // repaired replica must NOT claim, because it holds nothing produced
    // under it.
    let source_history = [
        EpochStart {
            epoch: 3,
            start_offset: 0,
        },
        EpochStart {
            epoch: 5,
            start_offset: prefix_end / 2,
        },
        EpochStart {
            epoch: 7,
            start_offset: prefix_end,
        },
    ];
    let journal_path = h.destination.path().join("fencing-epochs");
    let kept =
        install_transferred_history(&Env::real(), &journal_path, &source_history, prefix_end)
            .expect("install the source's history alongside the transferred prefix");
    assert_eq!(
        kept, 2,
        "the entry at the sealed end is not this replica's to claim"
    );

    let set = SegmentSet::adopt_in(&Env::real(), h.destination.path(), Uuid::from_u128(0xFACE))
        .expect("the prefix with a journal beside it must still adopt");
    assert_eq!(set.next_offset(), prefix_end);
    drop(set);

    // The reconciliation the next fence will run: this replica's history
    // against the source's.
    let mut journal = FencingEpochJournal::open(&journal_path).expect("reopen");
    assert_eq!(
        journal.compare_lineage(&source_history),
        Lineage::Agreed,
        "a repaired replica must be able to prove its lineage"
    );
    // And the replica's own first grant records cleanly at the adopted tail —
    // the append the truncated history exists to keep legal.
    journal
        .record(8, prefix_end)
        .expect("the next epoch adoption at the tail must be accepted");
}

/// A repair's ownership marker must not make the range unopenable.
///
/// `vtopctl node repair` writes a marker file into the destination so a retry
/// can tell "a previous repair owns this" from "somebody else's data", which is
/// what makes resuming an interrupted transfer safe. That file then sits in a
/// directory discovery scans, and discovery QUARANTINES anything it cannot
/// account for — a range with a quarantined bundle refuses to open at all. So
/// the marker would have turned a resumable repair into an unopenable range,
/// which is a worse failure than the one it prevents.
///
/// Asserted rather than assumed, because the failure would appear only on the
/// retry path — the case that is hardest to reach and least often exercised.
#[test]
fn a_repair_ownership_marker_does_not_quarantine_the_range() {
    let h = harness();
    let receiver = SegmentReceiver::open(&Env::real(), h.destination.path()).unwrap();
    transfer(&h, &receiver).unwrap();

    std::fs::write(
        h.destination.path().join(".vtop-repair-owned"),
        b"vtop repair\n",
    )
    .unwrap();
    // The lock outlives the run that took it — an advisory `flock` leaves the
    // file behind — so it is present in every repaired directory a node is
    // then started against. Quarantining it would turn a completed repair into
    // an unopenable range.
    std::fs::write(h.destination.path().join(".vtop-repair-lock"), b"").unwrap();

    let catalog = StartupCatalog::discover(h.destination.path()).unwrap();
    assert!(
        catalog.quarantined.is_empty(),
        "the marker must be ignorable, not quarantining: {:?}",
        catalog.quarantined
    );

    SegmentSet::adopt_in(&Env::real(), h.destination.path(), Uuid::from_u128(0xAA11))
        .expect("a marked directory must still adopt");
}

/// A condemned range refuses to open, so a node cannot simply be restarted
/// into serving a history its source disowned.
///
/// The verdict is written by `vtopctl node repair`, but an operator following
/// the documented workflow — repair, then start the replica — never runs repair
/// a second time, and neither does a supervisor or a rescheduled pod. If the
/// check lived only in the CLI the marker would bind nobody who actually starts
/// the node, which is everybody.
#[test]
fn a_condemned_range_refuses_to_open_and_to_be_adopted() {
    let h = harness();
    let receiver = SegmentReceiver::open(&Env::real(), h.destination.path()).unwrap();
    transfer(&h, &receiver).unwrap();

    // Healthy first, so the refusal below is caused by the verdict and not by a
    // directory that could never have opened.
    SegmentSet::adopt_in(&Env::real(), h.destination.path(), Uuid::from_u128(0xC0DE))
        .expect("the transferred prefix must be adoptable before it is condemned");

    std::fs::write(
        h.destination.path().join(vtop_log::CONDEMNED_MARKER),
        b"the source is at offset 900 but the adopted prefix already ends at 940",
    )
    .unwrap();

    let message = match SegmentSet::open_in(&Env::real(), h.destination.path()) {
        Ok(_) => panic!("a condemned range must not open for serving"),
        Err(error) => error.to_string(),
    };
    assert!(
        message.contains("CONDEMNED"),
        "the refusal must say why, not read as a layout problem: {message}"
    );
    assert!(
        message.contains("offset 900"),
        "the original finding must reach whoever is looking at the failure: {message}"
    );

    // And adoption too, so a second repair cannot quietly re-bless it.
    assert!(
        SegmentSet::adopt_in(&Env::real(), h.destination.path(), Uuid::from_u128(0xBEEF)).is_err(),
        "a condemned range must not be adoptable either"
    );
}
