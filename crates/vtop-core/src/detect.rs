//! Content-based telemetry format detection.
//!
//! When a stream does not declare its [`TelemetryFormat`] explicitly, the engine
//! samples the first records of a batch and infers the format. This lets one
//! pipeline handle CEF, JSON, JSON Lines, syslog, and arbitrary text without
//! per-source configuration, and lets downstream steps (object extension,
//! manifest `format`, and compression tuning) adapt to the data.

use crate::types::TelemetryFormat;

/// Number of leading records sampled when inferring a batch's format.
const SAMPLE: usize = 32;

// Index helpers so the counts array and the mapping never drift apart.
const N_CLASSES: usize = 6;
fn class_index(f: &TelemetryFormat) -> usize {
    match f {
        TelemetryFormat::Cef => 0,
        TelemetryFormat::Leef => 1,
        TelemetryFormat::Json => 2,
        TelemetryFormat::Jsonl => 3,
        TelemetryFormat::Syslog => 4,
        TelemetryFormat::Raw => 5,
    }
}
fn class_from_index(i: usize) -> TelemetryFormat {
    match i {
        0 => TelemetryFormat::Cef,
        1 => TelemetryFormat::Leef,
        2 => TelemetryFormat::Json,
        3 => TelemetryFormat::Jsonl,
        4 => TelemetryFormat::Syslog,
        _ => TelemetryFormat::Raw,
    }
}

/// Classify a single record (one line, no trailing newline) into a format.
///
/// CEF and LEEF are recognized even when wrapped in a syslog/timestamp header
/// (real-world events often look like `<134>... CEF:0|...`), because the
/// pipe-delimited `CEF:0|` / `LEEF:1.0|` token is a strong, specific signal.
pub fn detect_record(record: &[u8]) -> TelemetryFormat {
    let s = trim_ascii_start(record);
    if s.is_empty() {
        return TelemetryFormat::Raw;
    }

    // CEF / LEEF — check the specific pipe-delimited token first so a syslog
    // wrapper doesn't mask them.
    if s.starts_with(b"CEF:") || contains(s, b"CEF:0|") {
        return TelemetryFormat::Cef;
    }
    if s.starts_with(b"LEEF:") || contains(s, b"LEEF:1.0|") || contains(s, b"LEEF:2.0|") {
        return TelemetryFormat::Leef;
    }

    // Syslog with a PRI header: "<NNN>..." where NNN is 1-3 digits.
    if looks_like_syslog_pri(s) {
        return TelemetryFormat::Syslog;
    }

    // JSON object / array per line → JSON Lines candidate.
    let json_shaped = (s.first() == Some(&b'{') && s.last() == Some(&b'}'))
        || (s.first() == Some(&b'[') && s.last() == Some(&b']'));
    if json_shaped && serde_json::from_slice::<serde_json::Value>(s).is_ok() {
        return TelemetryFormat::Jsonl;
    }

    TelemetryFormat::Raw
}

/// Infer the format of a batch by sampling its first [`SAMPLE`] records and
/// taking the dominant classification.
///
/// A single whole-batch JSON document (one record that is valid JSON) is
/// reported as [`TelemetryFormat::Json`]; many JSON records are
/// [`TelemetryFormat::Jsonl`].
pub fn detect_batch(records: &[Vec<u8>]) -> TelemetryFormat {
    if records.is_empty() {
        return TelemetryFormat::Raw;
    }

    // Special case: exactly one record that is a JSON value → a JSON document.
    if records.len() == 1 {
        let only = trim_ascii_start(&records[0]);
        if serde_json::from_slice::<serde_json::Value>(only).is_ok() {
            return TelemetryFormat::Json;
        }
    }

    let mut counts = [0usize; N_CLASSES];
    for rec in records.iter().take(SAMPLE) {
        counts[class_index(&detect_record(rec))] += 1;
    }

    // Pick the dominant non-Raw class if it covers a majority of the sample;
    // otherwise fall back to Raw (don't guess on noisy input).
    let sampled = records.len().min(SAMPLE);
    let raw_idx = class_index(&TelemetryFormat::Raw);
    let (best_idx, best_count) = counts
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != raw_idx) // ignore Raw when choosing a positive signal
        .max_by_key(|(_, c)| **c)
        .map(|(i, c)| (i, *c))
        .unwrap_or((raw_idx, 0));

    if best_count * 2 > sampled {
        class_from_index(best_idx)
    } else {
        TelemetryFormat::Raw
    }
}

/// Naive substring search for byte slices.
fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() || needle.len() > haystack.len() {
        return needle.is_empty();
    }
    haystack.windows(needle.len()).any(|w| w == needle)
}

fn trim_ascii_start(b: &[u8]) -> &[u8] {
    let mut i = 0;
    while i < b.len() && b[i].is_ascii_whitespace() {
        i += 1;
    }
    &b[i..]
}

/// True if the slice begins with a syslog PRI header `<N>` / `<NN>` / `<NNN>`.
fn looks_like_syslog_pri(s: &[u8]) -> bool {
    if s.first() != Some(&b'<') {
        return false;
    }
    let mut digits = 0;
    let mut i = 1;
    while i < s.len() && s[i].is_ascii_digit() {
        digits += 1;
        i += 1;
    }
    (1..=3).contains(&digits) && s.get(i) == Some(&b'>')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn recs(lines: &[&str]) -> Vec<Vec<u8>> {
        lines.iter().map(|l| l.as_bytes().to_vec()).collect()
    }

    #[test]
    fn detects_cef() {
        let b = recs(&[
            "CEF:0|VTOP|Engine|1.0|100|Login|3|src=10.0.0.1",
            "CEF:0|VTOP|Engine|1.0|101|Login|6|src=10.0.0.2",
        ]);
        assert_eq!(detect_batch(&b), TelemetryFormat::Cef);
    }

    #[test]
    fn detects_syslog_wrapped_cef() {
        // Real-world CEF is often syslog/timestamp-framed.
        let b = recs(&[
            "<134>Nov 23 18:50:00 host JATP: CEF:0|JATP|Cortex|3.6|email|Phish|7|src=1.2.3.4",
            "2016-01-23 17:36:39.841+00 host CEF:0|JATP|Cortex|3.6|cnc|Trojan|7|src=5.6.7.8",
        ]);
        assert_eq!(detect_batch(&b), TelemetryFormat::Cef);
    }

    #[test]
    fn detects_leef() {
        let b = recs(&[
            "<134>Sep 24 16:23:36 host LEEF:1.0|Cyphort|Cortex|5.0|http|src=1.1.1.1\tdst=2.2.2.2",
            "<134>Sep 24 14:23:41 host LEEF:1.0|Cyphort|Cortex|5.0|third_party|usrName=admin",
        ]);
        assert_eq!(detect_batch(&b), TelemetryFormat::Leef);
    }

    #[test]
    fn detects_jsonl() {
        let b = recs(&[
            r#"{"ts":"t","event":"login","user":"a"}"#,
            r#"{"ts":"t","event":"logout","user":"b"}"#,
        ]);
        assert_eq!(detect_batch(&b), TelemetryFormat::Jsonl);
    }

    #[test]
    fn single_json_doc_is_json() {
        let b = recs(&[r#"{"a":1,"b":[1,2,3]}"#]);
        assert_eq!(detect_batch(&b), TelemetryFormat::Json);
    }

    #[test]
    fn detects_syslog_pri() {
        let b = recs(&[
            "<34>1 2026-06-18T15:00:01Z host app - - - msg one",
            "<38>1 2026-06-18T15:00:02Z host app - - - msg two",
        ]);
        assert_eq!(detect_batch(&b), TelemetryFormat::Syslog);
    }

    #[test]
    fn plain_text_is_raw() {
        let b = recs(&["just some log line", "another plain line"]);
        assert_eq!(detect_batch(&b), TelemetryFormat::Raw);
    }

    #[test]
    fn empty_is_raw() {
        assert_eq!(detect_batch(&[]), TelemetryFormat::Raw);
    }

    #[test]
    fn mixed_noise_falls_back_to_raw() {
        let b = recs(&["CEF:0|x", "plain", "another", "more text"]);
        // Only 1/4 is CEF → no majority → Raw.
        assert_eq!(detect_batch(&b), TelemetryFormat::Raw);
    }
}
