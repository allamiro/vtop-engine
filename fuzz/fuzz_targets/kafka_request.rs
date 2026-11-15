//! Fuzz the Kafka gateway's request decoders (`vtop-kafka`): any Kafka
//! client, or anything that can reach the gateway's port, sends these bytes.
//! The input is a request body as the gateway's `answer` receives it (after
//! the length prefix): the header is decoded first, and its api key and
//! version pick the body decoder, exactly as the gateway dispatches. The
//! RECORDS field of a decoded produce request is split into batches and each
//! batch decoded, the way the gateway's `split_batches` does. No input may
//! panic (#729).
//!
//! The record batch decoder checks a CRC-32C before it trusts anything after
//! it, so the raw input is also decoded as a RECORDS field with each batch's
//! CRC repaired, which lets the fuzzer reach the record parser itself.
#![no_main]

use libfuzzer_sys::fuzz_target;
use vtop_kafka::api::{
    decode_fetch, decode_init_producer_id, decode_list_offsets, decode_metadata, decode_produce,
};
use vtop_kafka::api_groups::{
    decode_find_coordinator, decode_heartbeat, decode_join_group, decode_leave_group,
    decode_offset_commit, decode_offset_fetch, decode_sync_group,
};
use vtop_kafka::records::crc32c;
use vtop_kafka::{ApiKey, Decoder, HeaderVerdict, RecordBatch, RequestHeader, WireError};

/// Record batch v2 layout: the CRC sits at 17..21 and covers 21..end; the
/// batch length (bytes after itself) at 8..12.
const CRC_OFFSET: usize = 17;
const CRC_COVERAGE_START: usize = 21;
const LENGTH_END: usize = 12;

/// Decode and, if it decodes, require the body to be consumed: the gateway
/// refuses a request with trailing bytes (`consumed` in gateway.rs).
fn whole<T>(decoded: Result<T, WireError>, d: &Decoder<'_>, api: &'static str) -> Option<T> {
    let value = decoded.ok()?;
    d.expect_consumed(api).ok()?;
    Some(value)
}

/// Every batch in a RECORDS field, as the gateway splits it. With
/// `repair_crc`, each batch's CRC is recomputed before it is decoded.
fn split_batches(mut bytes: &[u8], repair_crc: bool) {
    while bytes.len() >= LENGTH_END {
        let declared = i32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
        let Some(len) = usize::try_from(declared)
            .ok()
            .and_then(|n| n.checked_add(LENGTH_END))
            .filter(|n| *n <= bytes.len())
        else {
            return;
        };
        let mut batch = bytes[..len].to_vec();
        if repair_crc && batch.len() >= CRC_COVERAGE_START {
            let crc = crc32c(&batch[CRC_COVERAGE_START..]);
            batch[CRC_OFFSET..CRC_COVERAGE_START].copy_from_slice(&crc.to_be_bytes());
        }
        if RecordBatch::decode(&batch).is_err() {
            return;
        }
        bytes = &bytes[len..];
    }
}

fn request(data: &[u8]) {
    let mut d = Decoder::new(data);
    let Ok(HeaderVerdict::Serve(header)) = RequestHeader::decode(&mut d) else {
        return;
    };
    let Some(key) = ApiKey::from_i16(header.api_key) else {
        return;
    };
    let version = header.api_version;
    match key {
        ApiKey::ApiVersions => {}
        ApiKey::Metadata => drop(whole(decode_metadata(&mut d, version), &d, "Metadata")),
        ApiKey::Produce => {
            if let Some(produce) = whole(decode_produce(&mut d, version), &d, "Produce") {
                for topic in &produce.topics {
                    for partition in &topic.partitions {
                        if let Some(records) = &partition.records {
                            split_batches(records, false);
                        }
                    }
                }
            }
        }
        ApiKey::Fetch => drop(whole(decode_fetch(&mut d, version), &d, "Fetch")),
        ApiKey::ListOffsets => drop(whole(
            decode_list_offsets(&mut d, version),
            &d,
            "ListOffsets",
        )),
        ApiKey::InitProducerId => drop(whole(
            decode_init_producer_id(&mut d, version),
            &d,
            "InitProducerId",
        )),
        ApiKey::FindCoordinator => drop(whole(
            decode_find_coordinator(&mut d, version),
            &d,
            "FindCoordinator",
        )),
        ApiKey::JoinGroup => drop(whole(decode_join_group(&mut d, version), &d, "JoinGroup")),
        ApiKey::SyncGroup => drop(whole(decode_sync_group(&mut d, version), &d, "SyncGroup")),
        ApiKey::Heartbeat => drop(whole(decode_heartbeat(&mut d, version), &d, "Heartbeat")),
        ApiKey::LeaveGroup => drop(whole(decode_leave_group(&mut d, version), &d, "LeaveGroup")),
        ApiKey::OffsetCommit => drop(whole(
            decode_offset_commit(&mut d, version),
            &d,
            "OffsetCommit",
        )),
        ApiKey::OffsetFetch => drop(whole(
            decode_offset_fetch(&mut d, version),
            &d,
            "OffsetFetch",
        )),
    }
}

fuzz_target!(|data: &[u8]| {
    request(data);
    split_batches(data, false);
    split_batches(data, true);
});
