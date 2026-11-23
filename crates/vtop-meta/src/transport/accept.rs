//! Accepting connections without letting one failed `accept` end the listener.
//!
//! `accept` can fail for reasons that say nothing about the listener: the
//! process is out of file descriptors (`EMFILE`), a connection was reset
//! before it was accepted, or a kernel buffer is short. A loop that returns
//! on the first such error takes the whole plane down, and a peer that can
//! open enough sockets can cause the shortage on purpose. The listener keeps
//! serving instead, after a pause, because a failure that persists (no
//! descriptors left) would otherwise spin at full speed and make it worse.

use std::future::Future;
use std::time::Duration;

/// How long to wait after a failed `accept` before trying again.
pub(crate) const ACCEPT_RETRY_DELAY: Duration = Duration::from_millis(500);

/// Await `accept` until it succeeds, pausing `delay` after each failure and
/// writing one line to stderr for it, which names `plane`.
pub(crate) async fn accept_retrying<F, Fut, T>(plane: &str, delay: Duration, mut accept: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = std::io::Result<T>>,
{
    loop {
        match accept().await {
            Ok(accepted) => return accepted,
            Err(error) => {
                eprintln!("warning: {plane} accept failed ({error}); retrying");
                tokio::time::sleep(delay).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A failed accept is retried, not returned: the listener outlives it.
    #[tokio::test]
    async fn a_failed_accept_is_retried_and_the_next_one_is_served() {
        let mut failures_left = 3_u32;
        let mut calls = 0_u32;
        let accepted = accept_retrying("test plane", Duration::from_millis(1), || {
            calls += 1;
            let result = if failures_left > 0 {
                failures_left -= 1;
                Err(std::io::Error::from_raw_os_error(24)) // EMFILE
            } else {
                Ok("connection")
            };
            async move { result }
        })
        .await;
        assert_eq!(accepted, "connection");
        assert_eq!(calls, 4);
    }
}
