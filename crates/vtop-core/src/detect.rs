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

    // JSON object / array per line → JSON Lines candidate. Readers normally
    // strip `\n`, but CRLF leaves `\r` behind and callers can supply other
    // framing whitespace. Trim only for the JSON check so CEF/LEEF/syslog
    // detection retains its existing byte-level behavior (#105).
    let json = trim_ascii_end(s);
    let json_shaped = (json.first() == Some(&b'{') && json.last() == Some(&b'}'))
        || (json.first() == Some(&b'[') && json.last() == Some(&b']'));
    if json_shaped && serde_json::from_slice::<serde_json::Value>(json).is_ok() {
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

fn trim_ascii_end(b: &[u8]) -> &[u8] {
    let mut end = b.len();
    while end > 0 && b[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    &b[..end]
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

    // A stable-toolchain no-panic smoke test over adversarial byte strings. The
    // exhaustive version is the `detect_record` fuzz target (see fuzz/), which
    // needs nightly; this guards the never-panic contract on every normal test
    // run. detect.rs slices and inspects raw bytes, so boundary inputs (empty,
    // lone prefix bytes, invalid UTF-8, truncated markers) are the risk.
    #[test]
    fn detect_record_never_panics_on_adversarial_bytes() {
        let cases: &[&[u8]] = &[
            b"",
            b"C",
            b"CEF:",
            b"CEF:0|",
            b"LEEF:",
            b"<",
            b"<9999999999999999>",
            b"{",
            b"{\"",
            b"[",
            &[0x00],
            &[0xff, 0xfe, 0xfd],
            &[0x80, 0x80, 0x80], // invalid UTF-8 continuation bytes
            &[b'<', 0xff, b'>'], // syslog-ish prefix with invalid UTF-8
            &[b'C', b'E', b'F', b':', 0xff],
        ];
        for c in cases {
            // The only assertion is that this returns rather than panics.
            let _ = detect_record(c);
        }
        // Also hammer detect_batch with ragged/empty records.
        let batch: Vec<Vec<u8>> = vec![vec![], vec![0xff], b"CEF:0|".to_vec(), vec![]];
        let _ = detect_batch(&batch);
    }

    // The following tests were added to kill specific surviving mutants that
    // cargo-mutants reported in detect.rs (issue #25) — each pins a boundary the
    // earlier tests left unconstrained.

    #[test]
    fn json_shape_needs_both_delimiters() {
        // json_shaped requires the FIRST byte to open and the LAST to close.
        // A record with only one side is not JSON-shaped → Raw (kills the
        // `&&`→`||` and `==`→`!=` mutants on the shape check).
        assert_eq!(detect_record(b"{\"a\":1"), TelemetryFormat::Raw); // no closing }
        assert_eq!(detect_record(b"\"a\":1}"), TelemetryFormat::Raw); // no opening {
        assert_eq!(detect_record(b"[1,2"), TelemetryFormat::Raw); // no closing ]
        assert_eq!(detect_record(b"1,2]"), TelemetryFormat::Raw); // no opening [
        assert_eq!(detect_record(b"{\"a\":1}"), TelemetryFormat::Jsonl); // both → JSONL
    }

    #[test]
    fn exactly_half_is_not_a_majority() {
        // 2 of 4 CEF: best_count*2 == sampled, which is NOT a majority (`>`, not
        // `>=`) → Raw. Kills the `>`→`>=` mutant on the majority threshold.
        let b = recs(&["CEF:0|a", "CEF:0|b", "plain one", "plain two"]);
        assert_eq!(detect_batch(&b), TelemetryFormat::Raw);
    }

    #[test]
    fn three_of_five_is_a_majority() {
        // best_count=3, sampled=5: 3*2=6 > 5 (majority) but 3+2=5 is NOT > 5.
        // Kills the `*`→`+` mutant on the majority threshold.
        let b = recs(&["CEF:0|a", "CEF:0|b", "CEF:0|c", "plain one", "plain two"]);
        assert_eq!(detect_batch(&b), TelemetryFormat::Cef);
    }

    #[test]
    fn syslog_pri_must_be_well_formed() {
        // A PRI must be 1-3 digits AND immediately closed by '>'. These pin the
        // `&&`→`||` and digit-bound mutants in looks_like_syslog_pri.
        assert_eq!(detect_record(b"<12x> not syslog"), TelemetryFormat::Raw); // no '>' after digits
        assert_eq!(detect_record(b"<9999> too many"), TelemetryFormat::Raw); // 4 digits
        assert_eq!(detect_record(b"<> empty pri"), TelemetryFormat::Raw); // 0 digits
        assert_eq!(detect_record(b"<134>real syslog"), TelemetryFormat::Syslog);
    }

    #[test]
    fn leading_whitespace_is_trimmed_before_classifying() {
        // The format markers are only found after trim_ascii_start advances past
        // leading whitespace. Kills the loop-bound / `+=` mutants there.
        assert_eq!(detect_record(b"   CEF:0|x"), TelemetryFormat::Cef);
        assert_eq!(detect_record(b"\t\n <134>msg"), TelemetryFormat::Syslog);
    }

    // The tests below kill the mutants that survived the #25 round (issue
    // #103). The earlier shape-check tests used inputs that fail JSON parsing
    // anyway, so mutating the shape check changed nothing; these use inputs
    // that PARSE as JSON but are not shaped, and vice versa.

    #[test]
    fn class_index_roundtrips_through_class_from_index() {
        // detect_record never returns Json, so counts[Json] can never win the
        // majority vote and `class_from_index(2)` is unreachable through the
        // public API. Pin the mapping directly (kills `delete match arm 2`).
        for f in [
            TelemetryFormat::Cef,
            TelemetryFormat::Leef,
            TelemetryFormat::Json,
            TelemetryFormat::Jsonl,
            TelemetryFormat::Syslog,
            TelemetryFormat::Raw,
        ] {
            assert_eq!(class_from_index(class_index(&f)), f);
        }
    }

    #[test]
    fn json_array_line_is_jsonl() {
        // Arrays were never tested; this needs BOTH `[` first and `]` last
        // (kills the `==`→`!=` mutants on the array clause).
        assert_eq!(detect_record(b"[1,2,3]"), TelemetryFormat::Jsonl);
    }

    #[test]
    fn valid_json_that_is_not_object_or_array_shaped_is_raw() {
        // Bare scalars parse as JSON but are not `{...}`/`[...]` shaped, so the
        // shape check must gate the parse (kills `&&`→`||` between shape and
        // parse).
        assert_eq!(detect_record(b"123"), TelemetryFormat::Raw);
        assert_eq!(detect_record(b"\"quoted string\""), TelemetryFormat::Raw);
        assert_eq!(detect_record(b"true"), TelemetryFormat::Raw);
    }

    #[test]
    fn json_framing_whitespace_is_ignored() {
        // Line readers commonly leave `\r` from CRLF framing. JSON permits
        // surrounding whitespace, so all ASCII framing variants classify as
        // JSONL after the shape check trims the record end (#105).
        for record in [
            b"{\"a\":1}\t".as_slice(),
            b"{\"a\":1}\r".as_slice(),
            b"{\"a\":1}\r\n".as_slice(),
            b"  {\"a\":1}  ".as_slice(),
            b"[1,2]\t".as_slice(),
            b"[1,2]\r\n".as_slice(),
        ] {
            assert_eq!(detect_record(record), TelemetryFormat::Jsonl);
        }
    }

    #[test]
    fn trimming_json_framing_does_not_widen_the_shape_or_parse_gate() {
        // Trailing framing whitespace must not turn malformed structures or
        // valid JSON scalars into object/array JSONL records.
        for record in [
            b"{\"a\":1\r\n".as_slice(),
            b"{\"a\":}\r\n".as_slice(),
            b"[1,]\t".as_slice(),
            b"123\r\n".as_slice(),
            b"true\t".as_slice(),
            b"\"quoted\" \r".as_slice(),
        ] {
            assert_eq!(detect_record(record), TelemetryFormat::Raw);
        }
    }

    #[test]
    fn crlf_json_batch_detection_preserves_json_vs_jsonl() {
        let jsonl = vec![b"{\"a\":1}\r".to_vec(), b"{\"a\":2}\r".to_vec()];
        assert_eq!(detect_batch(&jsonl), TelemetryFormat::Jsonl);

        // A single valid JSON record remains a whole JSON document, including
        // surrounding whitespace; #105 changes only per-line classification.
        let document = vec![b" \t{\"a\":1}\r\n".to_vec()];
        assert_eq!(detect_batch(&document), TelemetryFormat::Json);
    }

    #[test]
    fn json_end_trimming_does_not_change_other_format_families() {
        assert_eq!(detect_record(b"CEF:0|x\r"), TelemetryFormat::Cef);
        assert_eq!(detect_record(b"LEEF:1.0|x\t"), TelemetryFormat::Leef);
        assert_eq!(detect_record(b"<134>message\r"), TelemetryFormat::Syslog);
        assert_eq!(detect_record(b"plain text\r"), TelemetryFormat::Raw);
        assert_eq!(detect_record(b" \t\r\n"), TelemetryFormat::Raw);
    }

    #[test]
    fn contains_handles_degenerate_needles() {
        // The call sites only pass non-empty literals, so the guard's edge
        // cases need direct tests. Empty needle: trivially true (the `||`→`&&`
        // mutant falls through to `windows(0)`, which panics). Equal length:
        // an exact match must be found (the `>`→`>=`/`==` mutants make the
        // guard reject it).
        assert!(contains(b"abc", b""));
        assert!(contains(b"CEF:0|", b"CEF:0|"));
        assert!(!contains(b"ab", b"abc"));
        assert!(contains(b"x CEF:0| y", b"CEF:0|"));
        assert!(!contains(b"x CEF y", b"CEF:0|"));
    }
}
