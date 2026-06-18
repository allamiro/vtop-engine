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

/// Classify a single record (one line, no trailing newline) into a format.
pub fn detect_record(record: &[u8]) -> TelemetryFormat {
    // Work on a trimmed view; ignore leading whitespace.
    let s = trim_ascii_start(record);
    if s.is_empty() {
        return TelemetryFormat::Raw;
    }

    // CEF: "CEF:0|Vendor|Product|..." (ArcSight Common Event Format).
    if s.starts_with(b"CEF:") {
        return TelemetryFormat::Cef;
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

    let mut counts = [0usize; 5]; // Cef, Json, Jsonl, Syslog, Raw
    for rec in records.iter().take(SAMPLE) {
        let idx = match detect_record(rec) {
            TelemetryFormat::Cef => 0,
            TelemetryFormat::Json => 1,
            TelemetryFormat::Jsonl => 2,
            TelemetryFormat::Syslog => 3,
            TelemetryFormat::Raw => 4,
        };
        counts[idx] += 1;
    }

    // Pick the dominant non-Raw class if it covers a majority of the sample;
    // otherwise fall back to Raw (don't guess on noisy input).
    let sampled = records.len().min(SAMPLE);
    let (best_idx, best_count) = counts
        .iter()
        .enumerate()
        .take(4) // ignore Raw when choosing a positive signal
        .max_by_key(|(_, c)| **c)
        .map(|(i, c)| (i, *c))
        .unwrap_or((4, 0));

    if best_count * 2 > sampled {
        match best_idx {
            0 => TelemetryFormat::Cef,
            1 => TelemetryFormat::Json,
            2 => TelemetryFormat::Jsonl,
            3 => TelemetryFormat::Syslog,
            _ => TelemetryFormat::Raw,
        }
    } else {
        TelemetryFormat::Raw
    }
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
