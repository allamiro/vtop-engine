//! A Kafka topic's id: which incarnation of the topic a range was read from.
//!
//! A topic deleted and created again under the same name numbers its offsets
//! from zero, so the name and an offset cannot tell the two logs apart. The
//! broker gives each incarnation its own UUID (Kafka 2.8 and later), and the
//! ledger records it in the progress marker so a restart resumes from the rows
//! of the topic that exists now and from no other (`vtop-state`,
//! `committed_kafka_offsets`).
//!
//! librdkafka reports the id only through the DescribeTopics admin call; the
//! metadata the `rdkafka` crate wraps (`Metadata`, `MetadataTopic`) carries
//! none, and the crate has no safe wrapper for DescribeTopics. This module is
//! that wrapper.
//!
//! UNSAFE. The only `unsafe` in the Kafka adapter is here, in [`describe`]: a
//! DescribeTopics request through `rdkafka-sys`, kept to one function so the
//! rest of the adapter stays safe code. What it relies on, each repeated at
//! its block:
//!
//! - the client pointer comes from a live `&BaseConsumer`, which keeps the
//!   client alive for the whole call, and librdkafka clients are thread safe;
//! - every object it creates (queue, options, topic collection, event) is
//!   destroyed exactly once on every path, by a guard, and no pointer
//!   outlives the call;
//! - pointers handed to librdkafka (the topic names) stay valid for the call,
//!   and librdkafka copies the names before returning;
//! - what it reads back (names, errors, uuids) is read only while the event
//!   that owns it is alive, and copied into owned values before the event is
//!   destroyed.
//!
//! A topic whose id is the all-zero UUID has none: a broker before 2.8, or
//! one that does not report it; it answers `None`, "unknown", and never an id
//! that could match another row. A topic the broker could not describe is
//! left out of the answer, and a failed call is an `Err`.

use std::collections::HashMap;
use std::ffi::{c_char, CStr, CString};
use std::time::Duration;

use rdkafka::bindings as rd;
use rdkafka::consumer::{BaseConsumer, Consumer};

/// base64url, no padding: how Kafka prints a topic id (`Uuid.toString`).
const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// Kafka's text form of a UUID given as its two 64-bit halves, or `None` for
/// the all-zero UUID, which is how a broker with no topic ids reports one.
pub(crate) fn uuid_text(most_significant: i64, least_significant: i64) -> Option<String> {
    if most_significant == 0 && least_significant == 0 {
        return None;
    }
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&most_significant.to_be_bytes());
    bytes[8..].copy_from_slice(&least_significant.to_be_bytes());
    let mut text = String::with_capacity(22);
    for chunk in bytes.chunks(3) {
        let n = chunk
            .iter()
            .enumerate()
            .fold(0u32, |n, (i, b)| n | (u32::from(*b) << (16 - 8 * i)));
        // 16 bytes are five whole groups and one byte: 22 characters, no pad.
        let chars = chunk.len() + 1;
        for i in 0..chars {
            text.push(char::from(ALPHABET[((n >> (18 - 6 * i)) & 0x3f) as usize]));
        }
    }
    Some(text)
}

/// Releases a librdkafka object on every path out of [`describe`].
struct Guard<T> {
    ptr: *mut T,
    destroy: unsafe extern "C" fn(*mut T),
}

impl<T> Drop for Guard<T> {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            // SAFETY: `ptr` came from the matching constructor, is non-null,
            // and is destroyed only here, once.
            unsafe { (self.destroy)(self.ptr) };
        }
    }
}

/// Ask the broker for the id of each of `topics`: the topic's id when the
/// broker reports one, `None` when it does not. A topic the broker could not
/// describe (unknown to it, or a lookup that failed) is not in the answer. An
/// `Err` is a failed call (no answer for any topic), with the reason.
pub(crate) fn describe(
    consumer: &BaseConsumer,
    topics: &[String],
    timeout: Duration,
) -> Result<HashMap<String, Option<String>>, String> {
    let mut answers: HashMap<String, Option<String>> = HashMap::new();
    // A name with an interior NUL cannot be asked for, and is not a Kafka
    // topic name; its answer is unknown.
    let names: Vec<CString> = topics
        .iter()
        .filter_map(|t| match CString::new(t.as_str()) {
            Ok(name) => Some(name),
            Err(_) => {
                answers.insert(t.clone(), None);
                None
            }
        })
        .collect();
    if names.is_empty() {
        return Ok(answers);
    }
    let mut name_ptrs: Vec<*const c_char> = names.iter().map(|n| n.as_ptr()).collect();
    let timeout_ms = i32::try_from(timeout.as_millis()).unwrap_or(i32::MAX);
    let rk = consumer.client().native_ptr();

    // SAFETY: `rk` is the native client of `consumer`, borrowed for this
    // whole call, so it is alive; librdkafka clients are thread safe. Each
    // constructor below returns an owned object that its guard destroys
    // exactly once. `name_ptrs` points into `names`, which outlives the
    // DescribeTopics call, and librdkafka copies the names when
    // `rd_kafka_TopicCollection_of_topic_names` returns.
    unsafe {
        let queue = Guard {
            ptr: rd::rd_kafka_queue_new(rk),
            destroy: rd::rd_kafka_queue_destroy,
        };
        if queue.ptr.is_null() {
            return Err("could not create a queue for the request".into());
        }
        let options = Guard {
            ptr: rd::rd_kafka_AdminOptions_new(
                rk,
                rd::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_DESCRIBETOPICS,
            ),
            destroy: rd::rd_kafka_AdminOptions_destroy,
        };
        if options.ptr.is_null() {
            return Err("could not create the request's options".into());
        }
        let mut errstr = [0 as c_char; 256];
        let set = rd::rd_kafka_AdminOptions_set_request_timeout(
            options.ptr,
            timeout_ms,
            errstr.as_mut_ptr(),
            errstr.len(),
        );
        if set != rd::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
            return Err(format!(
                "could not set the request timeout: {}",
                CStr::from_ptr(errstr.as_ptr()).to_string_lossy()
            ));
        }
        let collection = Guard {
            ptr: rd::rd_kafka_TopicCollection_of_topic_names(
                name_ptrs.as_mut_ptr(),
                name_ptrs.len(),
            ),
            destroy: rd::rd_kafka_TopicCollection_destroy,
        };
        if collection.ptr.is_null() {
            return Err("could not build the topic collection".into());
        }
        rd::rd_kafka_DescribeTopics(rk, collection.ptr, options.ptr, queue.ptr);

        // The request has its own timeout; the poll waits a little longer so
        // the timeout's own error arrives as an event rather than as silence.
        let event = Guard {
            ptr: rd::rd_kafka_queue_poll(queue.ptr, timeout_ms.saturating_add(1000)),
            destroy: rd::rd_kafka_event_destroy,
        };
        if event.ptr.is_null() {
            return Err(format!(
                "the broker did not answer within {} ms",
                timeout.as_millis()
            ));
        }
        let failed = rd::rd_kafka_event_error(event.ptr);
        if failed != rd::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
            return Err(CStr::from_ptr(rd::rd_kafka_event_error_string(event.ptr))
                .to_string_lossy()
                .into_owned());
        }
        let result = rd::rd_kafka_event_DescribeTopics_result(event.ptr);
        if result.is_null() {
            return Err("the answer was not a DescribeTopics result".into());
        }
        let mut count = 0usize;
        let described = rd::rd_kafka_DescribeTopics_result_topics(result, &mut count);
        if described.is_null() {
            return Ok(answers);
        }
        for i in 0..count {
            // SAFETY (of the reads below): `described` holds `count` valid
            // descriptions owned by `event`, which is alive until this
            // function returns; everything is copied out before then.
            let topic = *described.add(i);
            let name_ptr = rd::rd_kafka_TopicDescription_name(topic);
            if name_ptr.is_null() {
                continue;
            }
            let name = CStr::from_ptr(name_ptr).to_string_lossy().into_owned();
            if !rd::rd_kafka_TopicDescription_error(topic).is_null() {
                // No answer for this topic (unknown to the broker, or a
                // lookup that failed): not "the broker has no id for it".
                continue;
            }
            let uuid = rd::rd_kafka_TopicDescription_topic_id(topic);
            let id = if uuid.is_null() {
                None
            } else {
                uuid_text(
                    rd::rd_kafka_Uuid_most_significant_bits(uuid),
                    rd::rd_kafka_Uuid_least_significant_bits(uuid),
                )
            };
            answers.insert(name, id);
        }
    }
    Ok(answers)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Kafka prints `Uuid.ZERO_UUID` as `AAAAAAAAAAAAAAAAAAAAAA`, and a broker
    /// with no topic ids reports it: unknown, not an id.
    #[test]
    fn the_zero_uuid_is_no_id() {
        assert_eq!(uuid_text(0, 0), None);
    }

    /// The text matches Kafka's own: base64url of the 16 bytes, most
    /// significant half first, 22 characters, no padding. The expected
    /// strings are `base64.urlsafe_b64encode(struct.pack('>qq', m, l))` with
    /// the padding stripped.
    #[test]
    fn a_uuid_prints_as_kafka_prints_it() {
        for (m, l, want) in [
            (0, 1, "AAAAAAAAAAAAAAAAAAAAAQ"),
            (1, 0, "AAAAAAAAAAEAAAAAAAAAAA"),
            (-1, -1, "_____________________w"),
            (
                0x0123_4567_89ab_cdef,
                -0x0123_4567_89ab_cdef,
                "ASNFZ4mrze_-3LqYdlQyEQ",
            ),
            (i64::MIN, i64::MAX, "gAAAAAAAAAB__________w"),
        ] {
            assert_eq!(uuid_text(m, l).as_deref(), Some(want), "({m}, {l})");
        }
    }
}
