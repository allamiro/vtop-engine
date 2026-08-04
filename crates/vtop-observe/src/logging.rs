//! One definition of "structured logs" for every VTOP process (#224).
//!
//! JSON is selected by an explicit argument OR `VTOP_LOG_FORMAT=json`. The env
//! form lets a container opt into structured logs without changing the
//! entrypoint, so a log pipeline (Alloy -> Loki) gets parseable
//! `{"level":...,"records":...}` lines instead of pretty text. In pretty mode
//! ANSI colour is emitted ONLY to a real terminal: writing escape codes to a
//! pipe (a container's captured stderr) corrupts every downstream parser — a
//! `level=~"WARN"` filter or `| logfmt` then matches nothing because the field
//! names are wrapped in `\e[3m…\e[0m`.
//!
//! Logs go to STDERR so they never collide with command output on STDOUT
//! (notably machine-readable `--json` payloads, and the live-cluster ready
//! markers the chaos harness parses).

/// Environment variable that selects the log encoding.
pub const FORMAT_ENV: &str = "VTOP_LOG_FORMAT";

/// Whether `VTOP_LOG_FORMAT` asks for JSON.
pub fn json_requested() -> bool {
    std::env::var(FORMAT_ENV)
        .map(|v| v.eq_ignore_ascii_case("json"))
        .unwrap_or(false)
}

/// Install the global subscriber.
///
/// `level` is the fallback filter, used only when `RUST_LOG` is unset.
/// `RUST_LOG` winning is deliberate and is the conventional Rust escape hatch:
/// an operator debugging a running deployment must be able to raise verbosity
/// without editing a config file or the entrypoint that passed a flag.
///
/// Installing twice is not an error: the second call is a no-op because a
/// subscriber is already in place, which is the outcome the caller wanted
/// anyway. Panicking there would let a telemetry detail abort a process whose
/// logging is, by definition, already working.
pub fn init(level: &str, json: bool) {
    use std::io::IsTerminal;
    use tracing_subscriber::EnvFilter;

    let json = json || json_requested();
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level.to_lowercase()));
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr);
    if json {
        let _ = builder.json().with_current_span(false).try_init();
    } else {
        let _ = builder
            .with_ansi(std::io::stderr().is_terminal())
            .try_init();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `VTOP_LOG_FORMAT` is process-wide; these tests mutate it, so they must
    /// not run concurrently with each other.
    fn env_lock() -> &'static std::sync::Mutex<()> {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
    }

    #[test]
    fn json_is_opt_in_and_case_insensitive() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        std::env::remove_var(FORMAT_ENV);
        assert!(!json_requested(), "pretty text is the default");
        std::env::set_var(FORMAT_ENV, "JSON");
        assert!(
            json_requested(),
            "a container setting JSON in any case must get structured logs"
        );
        std::env::set_var(FORMAT_ENV, "text");
        assert!(!json_requested());
        std::env::remove_var(FORMAT_ENV);
    }

    #[test]
    fn init_twice_does_not_panic() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        init("info", false);
        // A second init (e.g. a library helper plus a main) must be inert, not
        // fatal.
        init("debug", true);
    }
}
