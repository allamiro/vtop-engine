//! Event time carried by a batch (#559).
//!
//! A batch's `first_timestamp` / `last_timestamp` are the EARLIEST and LATEST
//! record times the source reported, in RFC 3339 UTC. Sources that know a
//! record's time (Kafka message timestamps) report them; the others report
//! nothing, and the fields stay `None`. They are not "the first and last
//! record's times": producer-supplied timestamps arrive out of order, so a
//! window's range is a minimum and a maximum.

use chrono::{DateTime, TimeZone, Utc};

/// Milliseconds since the epoch as RFC 3339 UTC with millisecond precision, or
/// `None` for a value no clock produces (negative: Kafka's "no timestamp").
pub fn format_millis(ms: i64) -> Option<String> {
    if ms < 0 {
        return None;
    }
    Utc.timestamp_millis_opt(ms)
        .single()
        .map(|t| t.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
}

/// Parse a reported time; `None` for a value that is not RFC 3339.
pub fn parse(t: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(t)
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

/// The earlier of two reported times. An unparseable value never wins over a
/// parseable one, so one malformed report cannot become a window's minimum.
pub fn earliest(a: Option<String>, b: Option<String>) -> Option<String> {
    pick(a, b, |x, y| x <= y)
}

/// The later of two reported times (see [`earliest`]).
pub fn latest(a: Option<String>, b: Option<String>) -> Option<String> {
    pick(a, b, |x, y| x >= y)
}

fn pick(
    a: Option<String>,
    b: Option<String>,
    keep_a: impl Fn(DateTime<Utc>, DateTime<Utc>) -> bool,
) -> Option<String> {
    match (a, b) {
        (None, other) | (other, None) => other,
        (Some(a), Some(b)) => match (parse(&a), parse(&b)) {
            (Some(x), Some(y)) => Some(if keep_a(x, y) { a } else { b }),
            (Some(_), None) => Some(a),
            (None, Some(_)) => Some(b),
            (None, None) => Some(a),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn millis_format_as_utc_and_negative_is_no_time() {
        assert_eq!(
            format_millis(1_767_225_600_123).as_deref(),
            Some("2026-01-01T00:00:00.123Z")
        );
        assert_eq!(format_millis(-1), None);
    }

    #[test]
    fn a_window_keeps_its_minimum_and_maximum_not_its_first_and_last() {
        let t = |s: &str| Some(s.to_string());
        // Out of order, as producer timestamps arrive.
        let reads = [
            "2026-01-01T10:00:00.000Z",
            "2026-01-01T09:00:00.000Z",
            "2026-01-01T11:00:00.000Z",
        ];
        let (mut lo, mut hi) = (None, None);
        for r in reads {
            lo = earliest(lo, t(r));
            hi = latest(hi, t(r));
        }
        assert_eq!(lo, t("2026-01-01T09:00:00.000Z"));
        assert_eq!(hi, t("2026-01-01T11:00:00.000Z"));
        // Offsets compare as instants, not as text.
        assert_eq!(
            earliest(t("2026-01-01T10:00:00+02:00"), t("2026-01-01T09:00:00Z")),
            t("2026-01-01T10:00:00+02:00")
        );
        // A malformed report never wins.
        assert_eq!(
            earliest(t("yesterday"), t("2026-01-01T09:00:00Z")),
            t("2026-01-01T09:00:00Z")
        );
        assert_eq!(
            latest(None, t("2026-01-01T09:00:00Z")),
            t("2026-01-01T09:00:00Z")
        );
    }
}
