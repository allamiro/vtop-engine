//! Redaction for what the exporter writes into span statuses and log lines,
//! and the rate limit on its warnings.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// A message with its secrets taken out, for a span status or a log line:
///
/// * every URL is cut to `scheme://host[:port]`: the user-info can hold a
///   password, and the path and query string a signature (a presigned S3
///   URL) or a token;
/// * every run of 32 or more hex digits becomes `<hex>`: a MAC, a key or a
///   digest is no use to whoever reads a trace and can be a secret;
/// * every AWS access key id (`AKIA...`, `ASIA...`) becomes `<key-id>`.
pub fn redact(message: &str) -> String {
    let urls = redact_urls(message);
    let hex = redact_hex(&urls);
    redact_key_ids(&hex)
}

fn redact_urls(message: &str) -> String {
    let mut out = String::with_capacity(message.len());
    let mut rest = message;
    while let Some(at) = rest.find("://") {
        let before = &rest[..at];
        let scheme_start = before
            .rfind(|c: char| !(c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.')))
            .map_or(0, |i| i + 1);
        out.push_str(&before[..scheme_start]);
        let scheme = &before[scheme_start..];
        let after = &rest[at + 3..];
        let end = after
            .find(|c: char| c.is_whitespace() || matches!(c, '\'' | '"' | '<' | '>' | '`' | ')'))
            .unwrap_or(after.len());
        let url = &after[..end];
        let authority = url.split(['/', '?', '#']).next().unwrap_or_default();
        let host = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
        if scheme.is_empty() {
            out.push_str("://");
        } else {
            out.push_str(scheme);
            out.push_str("://");
        }
        out.push_str(host);
        rest = &after[end..];
    }
    out.push_str(rest);
    out
}

fn redact_hex(message: &str) -> String {
    let mut out = String::with_capacity(message.len());
    let mut run = String::new();
    let flush = |run: &mut String, out: &mut String| {
        if run.len() >= 32 {
            out.push_str("<hex>");
        } else {
            out.push_str(run);
        }
        run.clear();
    };
    for c in message.chars() {
        if c.is_ascii_hexdigit() {
            run.push(c);
        } else {
            flush(&mut run, &mut out);
            out.push(c);
        }
    }
    flush(&mut run, &mut out);
    out
}

fn redact_key_ids(message: &str) -> String {
    let bytes = message.as_bytes();
    let mut out = String::with_capacity(message.len());
    let mut i = 0;
    while i < bytes.len() {
        let is_key = i + 20 <= bytes.len()
            && (bytes[i..].starts_with(b"AKIA") || bytes[i..].starts_with(b"ASIA"))
            && bytes[i + 4..i + 20]
                .iter()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
            && (i == 0 || !bytes[i - 1].is_ascii_alphanumeric());
        if is_key {
            out.push_str("<key-id>");
            i += 20;
        } else {
            let c = message[i..].chars().next().unwrap_or_default();
            out.push(c);
            i += c.len_utf8().max(1);
        }
    }
    out
}

/// A warning that is logged at most once per interval, carrying the count of
/// what happened since it was last logged.
pub(crate) struct RateLimited {
    interval: Duration,
    last: Mutex<Option<Instant>>,
    since: AtomicU64,
}

impl RateLimited {
    pub(crate) const fn new(interval: Duration) -> Self {
        Self {
            interval,
            last: Mutex::new(None),
            since: AtomicU64::new(0),
        }
    }

    /// Count `n` more; returns the total since the last warning when a warning
    /// is due now (the first one always is), and resets it.
    pub(crate) fn note(&self, n: u64) -> Option<u64> {
        self.since.fetch_add(n, Ordering::Relaxed);
        let mut last = self.last.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        if last.is_some_and(|at| now.duration_since(at) < self.interval) {
            return None;
        }
        *last = Some(now);
        Some(self.since.swap(0, Ordering::Relaxed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls_keep_their_host_only() {
        assert_eq!(
            redact("PUT 'https://AKIA0000000000000000:pw@bucket.s3.example:9000/k/o?X-Amz-Signature=abc' failed"),
            "PUT 'https://bucket.s3.example:9000' failed"
        );
        assert_eq!(
            redact("dispatch failure: http://minio:9000/telemetry/obj.jsonl.gz?versionId=7"),
            "dispatch failure: http://minio:9000"
        );
        assert_eq!(redact("no url here"), "no url here");
    }

    #[test]
    fn digests_macs_and_key_ids_are_removed() {
        let mac = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";
        assert_eq!(
            redact(&format!("manifest MAC mismatch: expected {mac}, got {mac}")),
            "manifest MAC mismatch: expected <hex>, got <hex>"
        );
        assert_eq!(
            redact("credentials for AKIAIOSFODNN7EXAMPLE rejected"),
            "credentials for <key-id> rejected"
        );
        assert_eq!(
            redact("batch b0-47 ok"),
            "batch b0-47 ok",
            "short hex is kept"
        );
    }

    #[test]
    fn a_rate_limited_warning_carries_the_count_since_the_last_one() {
        let limit = RateLimited::new(Duration::from_secs(60));
        assert_eq!(limit.note(3), Some(3), "the first warning is due at once");
        assert_eq!(limit.note(2), None);
        assert_eq!(limit.note(5), None);
        let fast = RateLimited::new(Duration::ZERO);
        assert_eq!(fast.note(1), Some(1));
        assert_eq!(fast.note(4), Some(4));
    }
}
