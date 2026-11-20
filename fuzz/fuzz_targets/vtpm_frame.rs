//! Fuzz the VTPM transport (`vtop-meta`): the frames metadata peers and
//! admin clients exchange (votes, log appends, snapshot installs, the admin
//! reads and proposals). A peer or a client on the metadata port sends these
//! bytes, so no input may panic the decoder of the frame or of any payload
//! (#729).
//!
//! The input goes through the frame decoder as sent and with its checksum
//! repaired; a frame that decodes has its payload decoded by its kind, the
//! way the peer and admin servers dispatch it. The new frames #706 adds are
//! fuzzed by its `meta_decode` target, not here.
#![no_main]

use libfuzzer_sys::fuzz_target;
use vtop_fuzz::{run_ready, with_frame_checksum};
use vtop_meta::transport::*;

/// VTPM header: magic 4, version 2, kind 2, payload length 4, checksum 32.
const HEADER_LEN: usize = 44;
const CHECKSUM_OFFSET: usize = 12;

/// Decode `payload` as the message `kind` names. Unknown kinds decode as
/// nothing, as the servers refuse them.
fn decode_payload(kind: u16, payload: &[u8]) {
    match kind {
        KIND_VOTE_REQ => drop(PeerVoteRequest::decode(payload)),
        KIND_VOTE_RESP => drop(PeerVoteResponse::decode(payload)),
        KIND_APPEND_REQ => drop(PeerAppendRequest::decode(payload)),
        KIND_APPEND_RESP => drop(PeerAppendResponse::decode(payload)),
        KIND_INSTALL_REQ => drop(PeerInstallRequest::decode(payload)),
        KIND_INSTALL_RESP => drop(PeerInstallResponse::decode(payload)),
        KIND_ADMIN_STATUS_REQ => drop(AdminStatusRequest::decode(payload)),
        KIND_ADMIN_STATUS_RESP => drop(AdminStatusResponse::decode(payload)),
        KIND_ADMIN_PROPOSE_REQ => drop(AdminProposeRequest::decode(payload)),
        KIND_ADMIN_PROPOSE_RESP => drop(AdminProposeResponse::decode(payload)),
        KIND_ADMIN_ERROR => drop(AdminError::decode(payload)),
        KIND_ADMIN_INIT_REQ => drop(AdminInitRequest::decode(payload)),
        KIND_ADMIN_ADD_LEARNER_REQ => drop(AdminAddLearnerRequest::decode(payload)),
        KIND_ADMIN_CHANGE_MEMBERSHIP_REQ => drop(AdminChangeMembershipRequest::decode(payload)),
        KIND_ADMIN_MEMBERSHIP_RESP => drop(AdminMembershipResponse::decode(payload)),
        KIND_ADMIN_READ_RANGE_LEASE_REQ => drop(AdminReadRangeLeaseRequest::decode(payload)),
        KIND_ADMIN_READ_RANGE_LEASE_RESP => drop(AdminReadRangeLeaseResponse::decode(payload)),
        KIND_ADMIN_READ_SEGMENT_PLACEMENT_REQ => {
            drop(AdminReadSegmentPlacementRequest::decode(payload))
        }
        KIND_ADMIN_READ_SEGMENT_PLACEMENT_RESP => {
            drop(AdminReadSegmentPlacementResponse::decode(payload))
        }
        KIND_ADMIN_READ_RANGE_TRANSITIONS_REQ => {
            drop(AdminReadRangeTransitionsRequest::decode(payload))
        }
        KIND_ADMIN_READ_RANGE_TRANSITIONS_RESP => {
            drop(AdminReadRangeTransitionsResponse::decode(payload))
        }
        KIND_ADMIN_READ_GROUP_CURSOR_REQ => drop(AdminReadGroupCursorRequest::decode(payload)),
        KIND_ADMIN_READ_GROUP_CURSOR_RESP => drop(AdminReadGroupCursorResponse::decode(payload)),
        KIND_ADMIN_READ_RANGE_REPLICA_SET_REQ => {
            drop(AdminReadRangeReplicaSetRequest::decode(payload))
        }
        KIND_ADMIN_READ_RANGE_REPLICA_SET_RESP => {
            drop(AdminReadRangeReplicaSetResponse::decode(payload))
        }
        KIND_ADMIN_READ_CLUSTER_VERSIONS_REQ => {
            drop(AdminReadClusterVersionsRequest::decode(payload))
        }
        KIND_ADMIN_READ_CLUSTER_VERSIONS_RESP => {
            drop(AdminReadClusterVersionsResponse::decode(payload))
        }
        KIND_ADMIN_READ_PROMOTION_ATTESTATION_REQ => {
            drop(AdminReadPromotionAttestationRequest::decode(payload))
        }
        KIND_ADMIN_READ_PROMOTION_ATTESTATION_RESP => {
            drop(AdminReadPromotionAttestationResponse::decode(payload))
        }
        _ => {}
    }
}

fn check(bytes: &[u8]) {
    if let Ok(frame) = VtpmFrame::decode(bytes) {
        let encoded = frame.encode().expect("a decoded frame re-encodes");
        assert_eq!(
            encoded, bytes,
            "VTPM frame did not re-encode to its own bytes"
        );
        decode_payload(frame.kind, &frame.payload);
    }
    let mut reader = bytes;
    let _ = run_ready(read_frame(&mut reader));
}

fuzz_target!(|data: &[u8]| {
    check(data);
    if data.len() >= HEADER_LEN {
        check(&with_frame_checksum(data, CHECKSUM_OFFSET, HEADER_LEN));
    }
});
