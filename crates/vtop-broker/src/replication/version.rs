//! The replica plane's version handshake (#634).
//!
//! A replica session used to open with its first request, so neither side
//! could tell which release the other ran. Safety fixes that change replica
//! behaviour (#611: a fenced replica that keeps records a candidate merely
//! lacks) then held only where both ends were upgraded, and a later wire
//! change (#612's retained base in the fence reply) had nothing to ride on.
//!
//! Now the side that dials opens with a [`ReplicaHello`]. Each side judges
//! the other against the cluster's FINALIZED feature level - the one
//! metadata recorded, as this node last read it ([`ClusterFeatureLevel`]):
//!
//! * a peer that answers with a [`ReplicaHelloAccept`] speaks the handshake;
//!   both continue at the lower protocol, and the peer is refused if it
//!   implements less than the finalized level;
//! * a peer that answers with the typed `InvalidRequest` every older replica
//!   dispatcher gives a hello predates it; the session continues at
//!   [`REPLICA_PROTOCOL_LEGACY`], today's frames, until the finalized level
//!   rises above [`LEGACY_PEERS_ACCEPTED_THROUGH`], and is refused after.
//!
//! The same judgement runs on the serving side: a session whose first frame
//! is not a hello comes from a peer that predates it.
//!
//! A node that has not read the level (a static range with no metadata, or
//! one that has not reached it yet) judges against level 0 and refuses
//! nobody. That keeps static and plaintext ranges working exactly as they
//! did; the guarantee the handshake adds is enforced by every node that
//! knows the level, and the metadata gate (`vtopctl meta enable-features`)
//! refuses to raise it while a counted node has not reported it.

use crate::{BrokerError, BrokerResult};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use uuid::Uuid;
use vtop_protocol::{
    negotiate_replica_protocol, read_frame, write_frame, ErrorCode, ErrorResponse, Message,
    ReplicaHello, ReplicaHelloAccept, WireFrame, REPLICA_PROTOCOL_HANDSHAKE,
    REPLICA_PROTOCOL_LEGACY, REPLICA_PROTOCOL_MAX,
};

/// The highest finalized level at which a peer that predates the handshake
/// is still served.
///
/// The level that lets data nodes report what they run
/// ([`vtop_meta::NODE_VERSION_REPORTS_FEATURE_LEVEL`]) is the last one raised
/// without hearing from them - nothing could vouch for them before it - and
/// it changes nothing on the replica plane. So raising it must not cut a
/// peer off; every level above it is raised only once every live range
/// member reported it, which a peer before the handshake cannot do.
pub const LEGACY_PEERS_ACCEPTED_THROUGH: u32 = vtop_meta::NODE_VERSION_REPORTS_FEATURE_LEVEL;

/// The cluster's finalized feature level as this node last read it (#634),
/// shared by everything on the node that opens or serves a replica session.
///
/// Only ever rises: a level is never lowered, so every value read is a lower
/// bound on the true one, and keeping the highest is never wrong. Zero means
/// the node has not read one, and refuses nobody.
#[derive(Clone, Debug, Default)]
pub struct ClusterFeatureLevel(Arc<AtomicU32>);

impl ClusterFeatureLevel {
    /// A level not yet read.
    pub fn unknown() -> Self {
        Self::default()
    }

    /// Record a level metadata reported. A lower one than already seen is a
    /// stale read and changes nothing.
    pub fn observe(&self, level: u32) {
        self.0.fetch_max(level, Ordering::AcqRel);
    }

    pub fn get(&self) -> u32 {
        self.0.load(Ordering::Acquire)
    }
}

/// What a node brings to every replica session it opens or serves.
#[derive(Clone, Debug, Default)]
pub struct ReplicaVersionPolicy {
    /// Named in the hello. Informational: on a TLS plane the certificate is
    /// what identifies a peer, and a plaintext plane has no identity at all.
    pub node_id: Uuid,
    pub cluster_level: ClusterFeatureLevel,
}

impl ReplicaVersionPolicy {
    pub fn new(node_id: Uuid, cluster_level: ClusterFeatureLevel) -> Self {
        Self {
            node_id,
            cluster_level,
        }
    }
}

/// What a peer turned out to speak.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerVersion {
    /// It answered (or opened) without the handshake: a release before #634.
    Legacy,
    Negotiated {
        /// What both sides speak for the rest of the session.
        protocol: u16,
        /// The highest feature level the peer implements.
        supported_feature_level: u32,
    },
}

impl PeerVersion {
    /// The replica protocol this session runs at.
    pub fn protocol(&self) -> u16 {
        match self {
            PeerVersion::Legacy => REPLICA_PROTOCOL_LEGACY,
            PeerVersion::Negotiated { protocol, .. } => *protocol,
        }
    }
}

/// This build's side of the handshake.
pub fn local_hello(node_id: Uuid) -> ReplicaHello {
    ReplicaHello {
        node_id,
        min_protocol: REPLICA_PROTOCOL_HANDSHAKE,
        max_protocol: REPLICA_PROTOCOL_MAX,
        supported_feature_level: vtop_meta::SUPPORTED_FEATURE_LEVEL,
    }
}

/// Whether a peer may take part in a cluster finalized at `finalized`.
/// `Err` says why not, in words an operator can act on.
pub fn judge_peer(finalized: u32, peer: PeerVersion) -> Result<(), String> {
    match peer {
        PeerVersion::Legacy if finalized > LEGACY_PEERS_ACCEPTED_THROUGH => Err(format!(
            "the cluster's feature level is {finalized}, and this replica peer predates the \
             replica version handshake (#634), so it cannot run that level; upgrade it"
        )),
        PeerVersion::Negotiated {
            supported_feature_level,
            ..
        } if supported_feature_level < finalized => Err(format!(
            "the cluster's feature level is {finalized}, and this replica peer implements only \
             {supported_feature_level}; upgrade it"
        )),
        _ => Ok(()),
    }
}

/// Open a replica session from the dialing side: send the hello, read the
/// answer, and judge the peer against `level`.
///
/// The ONLY first frame a dialer writes, so it must run before any request:
/// a peer that speaks the handshake refuses a session that skips it once
/// the level demands it.
pub async fn open_session<S>(
    stream: &mut S,
    policy: &ReplicaVersionPolicy,
) -> BrokerResult<PeerVersion>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let hello = local_hello(policy.node_id);
    write_frame(
        stream,
        &WireFrame {
            request_id: 0,
            stream_id: 0,
            message: hello.to_message(),
        },
        super::REPLICA_LIMITS,
    )
    .await?;
    let Some(reply) = read_frame(stream, super::REPLICA_LIMITS).await? else {
        return Err(BrokerError::ReplicaVersion(
            "the replica peer closed the session without answering its version hello".to_owned(),
        ));
    };
    let peer = match &reply.message {
        message if ReplicaHelloAccept::from_message(message).is_some() => {
            let accept = ReplicaHelloAccept::from_message(message).expect("checked above");
            // The peer chose; check it chose something BOTH of us speak - in
            // this node's offer, and at or below the peer's own maximum - so
            // a confused or hostile answer cannot move this side to a
            // protocol one end does not implement (review).
            let selected = accept.selected_protocol;
            if !(hello.min_protocol..=hello.max_protocol).contains(&selected)
                || selected > accept.max_protocol
            {
                return Err(BrokerError::ReplicaVersion(format!(
                    "the replica peer selected protocol {selected}, outside the {}..={} this \
                     node offered or above the {} it says it implements",
                    hello.min_protocol, hello.max_protocol, accept.max_protocol
                )));
            }
            PeerVersion::Negotiated {
                protocol: accept.selected_protocol,
                supported_feature_level: accept.supported_feature_level,
            }
        }
        // What every replica dispatcher before the handshake answers a hello
        // with, from its catch-all arm, keeping the session open.
        Message::Error(ErrorResponse {
            code: ErrorCode::InvalidRequest,
            ..
        }) => PeerVersion::Legacy,
        Message::Error(ErrorResponse {
            code: ErrorCode::UnsupportedVersion,
            message,
            ..
        }) => {
            return Err(BrokerError::ReplicaVersion(format!(
                "the replica peer refused this node: {message}"
            )))
        }
        other => {
            return Err(BrokerError::ReplicaVersion(format!(
                "unexpected answer to a replica version hello: {other:?}"
            )))
        }
    };
    judge_peer(policy.cluster_level.get(), peer).map_err(BrokerError::ReplicaVersion)?;
    Ok(peer)
}

/// The typed refusal a serving side answers a peer it will not run a
/// session with: `UnsupportedVersion`, not retryable, saying why.
pub(crate) fn refusal(request_id: u64, stream_id: u64, why: String) -> WireFrame {
    WireFrame {
        request_id,
        stream_id,
        message: Message::Error(ErrorResponse {
            code: ErrorCode::UnsupportedVersion,
            retryable: false,
            message: why,
        }),
    }
}

/// What the serving side made of a session's first frame.
pub(crate) enum Accepted {
    /// A hello, answered: the session continues at this version.
    Hello(PeerVersion),
    /// No hello: a dialer before the handshake, judged and accepted. The
    /// frame is its first request, still to be served.
    Legacy(Box<WireFrame>),
    /// Refused; the refusal has been written and the session must close.
    Refused(String),
}

/// Judge a session's first frame on the serving side, answering a hello.
pub(crate) async fn accept_session<S>(
    stream: &mut S,
    first: WireFrame,
    policy: &ReplicaVersionPolicy,
) -> BrokerResult<Accepted>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let refuse = refusal;
    let level = policy.cluster_level.get();
    let Some(hello) = ReplicaHello::from_message(&first.message) else {
        return match judge_peer(level, PeerVersion::Legacy) {
            Ok(()) => Ok(Accepted::Legacy(Box::new(first))),
            Err(why) => {
                write_frame(
                    stream,
                    &refuse(first.request_id, first.stream_id, why.clone()),
                    super::REPLICA_LIMITS,
                )
                .await?;
                Ok(Accepted::Refused(why))
            }
        };
    };
    let local = local_hello(policy.node_id);
    let verdict = match negotiate_replica_protocol(
        local.min_protocol,
        local.max_protocol,
        hello.min_protocol,
        hello.max_protocol,
    ) {
        None => Err(format!(
            "no replica protocol in common: this node speaks {}..={}, the peer {}..={}",
            local.min_protocol, local.max_protocol, hello.min_protocol, hello.max_protocol
        )),
        Some(protocol) => {
            let peer = PeerVersion::Negotiated {
                protocol,
                supported_feature_level: hello.supported_feature_level,
            };
            judge_peer(level, peer).map(|()| (protocol, peer))
        }
    };
    match verdict {
        Ok((protocol, peer)) => {
            write_frame(
                stream,
                &WireFrame {
                    request_id: first.request_id,
                    stream_id: first.stream_id,
                    message: ReplicaHelloAccept {
                        node_id: policy.node_id,
                        selected_protocol: protocol,
                        max_protocol: local.max_protocol,
                        supported_feature_level: local.supported_feature_level,
                    }
                    .to_message(),
                },
                super::REPLICA_LIMITS,
            )
            .await?;
            Ok(Accepted::Hello(peer))
        }
        Err(why) => {
            write_frame(
                stream,
                &refuse(first.request_id, first.stream_id, why.clone()),
                super::REPLICA_LIMITS,
            )
            .await?;
            Ok(Accepted::Refused(why))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The rule both sides apply, at each level that matters.
    #[test]
    fn a_peer_below_the_finalized_level_is_refused_and_one_at_it_is_not() {
        let at = |level| PeerVersion::Negotiated {
            protocol: REPLICA_PROTOCOL_HANDSHAKE,
            supported_feature_level: level,
        };
        // Unknown level, or the report level: nobody is refused, the legacy
        // peer included.
        for finalized in [0, LEGACY_PEERS_ACCEPTED_THROUGH] {
            assert!(judge_peer(finalized, PeerVersion::Legacy).is_ok());
            assert!(judge_peer(finalized, at(LEGACY_PEERS_ACCEPTED_THROUGH)).is_ok());
        }
        let above = LEGACY_PEERS_ACCEPTED_THROUGH + 1;
        let legacy = judge_peer(above, PeerVersion::Legacy).unwrap_err();
        assert!(legacy.contains("predates"), "{legacy}");
        let behind = judge_peer(above, at(above - 1)).unwrap_err();
        assert!(
            behind.contains(&format!("implements only {}", above - 1)),
            "{behind}"
        );
        assert!(judge_peer(above, at(above)).is_ok());
        assert!(judge_peer(above, at(above + 1)).is_ok());
    }

    #[test]
    fn the_level_only_rises() {
        let level = ClusterFeatureLevel::unknown();
        assert_eq!(level.get(), 0);
        level.observe(7);
        level.observe(6);
        assert_eq!(level.get(), 7, "a stale read never lowers it");
        let shared = level.clone();
        shared.observe(8);
        assert_eq!(level.get(), 8, "one view per node, shared");
    }
}
