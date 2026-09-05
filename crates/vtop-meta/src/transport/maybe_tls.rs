//! A stream that may or may not be wrapped in TLS.
//!
//! # Why this type has to exist
//!
//! Every plane hands a stream to [`crate::transport::wire::read_frame`] and
//! [`crate::transport::wire::write_frame`], which are already generic over
//! `AsyncRead`/`AsyncWrite`. The frame layer was never the obstacle to making
//! transport security optional — the CONSTRUCTION sites were, because each one
//! produces a concrete `TlsStream` and the type flows outward from there. This
//! enum is the seam: one type that is either, so a plane can be configured
//! rather than compiled one way.
//!
//! # What it deliberately does not do
//!
//! It does not decide anything. Whether a plane runs plaintext is a
//! configuration question answered before a connection exists; by the time a
//! stream is built the answer is already known. A type that sniffed the first
//! bytes to guess would turn a downgrade into something an attacker chooses,
//! which is the opposite of the point.
//!
//! Judging is not deciding (#294 slice 6). [`judge_dialect`] also looks at
//! the first byte — but only to say whether the peer AGREES with what the
//! plane was configured to be, and a peer that does not is refused by name,
//! never accommodated. Nothing is negotiated, so there is nothing for an
//! attacker to choose.

use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

/// Either a TLS-wrapped stream or the bare one underneath it.
///
/// `Box`ed on the TLS side because `tokio_rustls`'s stream is large and this
/// enum is passed by value through the accept path; an unboxed variant makes
/// every `MaybeTls` the size of the biggest one.
pub enum MaybeTls<S> {
    Tls(Box<tokio_rustls::TlsStream<S>>),
    Plain(S),
}

impl<S> MaybeTls<S> {
    /// The peer's certificate chain, or `None` when there is no TLS.
    ///
    /// `None` is not "no certificate was sent" — a TLS connection here always
    /// carries one, because the acceptor is built with a client verifier. It
    /// means there is no TLS at all, and therefore NO IDENTITY. Callers must
    /// treat that as an unauthenticated peer rather than as an authenticated
    /// one whose name they failed to read; the two want opposite defaults.
    pub fn peer_certificates(&self) -> Option<&[rustls::pki_types::CertificateDer<'static>]> {
        match self {
            Self::Tls(stream) => {
                let (_, connection) = stream.get_ref();
                connection.peer_certificates()
            }
            Self::Plain(_) => None,
        }
    }

    /// True when bytes on this connection are encrypted.
    pub fn is_encrypted(&self) -> bool {
        matches!(self, Self::Tls(_))
    }
}

/// What the first byte on a freshly accepted socket says the peer is
/// speaking (#294 slice 6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireDialect {
    /// A TLS record of content type 22, a handshake: the only thing a TLS
    /// client ever sends first.
    Tls,
    /// A vtop frame. Every plane's frame opens with a magic whose first byte
    /// is `V` (`VTPM` on the metadata planes, `VTPW` on the broker's), so a
    /// plaintext client can never look like TLS, and TLS never like a frame.
    Frame,
    /// Neither (review): an HTTP probe, a port scan, line noise. Not judged
    /// — it is not a peer on the wrong transport, and calling it one would
    /// send an operator to change a configuration that is not wrong. It
    /// goes on to the plane's own handshake or frame error, as before.
    Other,
}

const TLS_HANDSHAKE_RECORD: u8 = 0x16;
const FRAME_MAGIC_FIRST: u8 = b'V';

/// Read the dialect WITHOUT consuming it: the byte stays for whichever reader
/// the plane hands the socket to next. A peer that closed before speaking is
/// an `UnexpectedEof`, which every plane already treats as a hang-up.
pub async fn sniff_dialect(tcp: &TcpStream) -> io::Result<WireDialect> {
    let mut first = [0_u8; 1];
    if tcp.peek(&mut first).await? == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "peer closed before speaking",
        ));
    }
    Ok(match first[0] {
        TLS_HANDSHAKE_RECORD => WireDialect::Tls,
        FRAME_MAGIC_FIRST => WireDialect::Frame,
        _ => WireDialect::Other,
    })
}

/// The refusal a plane gives a peer speaking the other transport, naming BOTH
/// sides (#294 slice 6): what the peer spoke, what the plane is configured to
/// be, and the two ways to make them agree. Before this the symptom was a
/// reset on one side and a bad magic or a handshake error on the other, and
/// nothing named the mismatch.
pub fn cross_mode_refusal(plane: &str, knob: &str, plane_is_tls: bool, peer: &str) -> String {
    if plane_is_tls {
        format!(
            "refusing {peer} on the {plane} plane: it sent a plaintext frame, and this plane is \
             `{knob}: tls` (the default); a plane is one transport in both directions — dial it \
             with `transport: tls`, or serve it with `{knob}: plaintext`"
        )
    } else {
        format!(
            "refusing {peer} on the {plane} plane: it opened a TLS handshake, and this plane is \
             `{knob}: plaintext`; a plane is one transport in both directions — dial it with \
             `transport: plaintext`, or serve it with `{knob}: tls`"
        )
    }
}

/// What [`judge_dialect`] found.
#[derive(Debug)]
pub enum DialectVerdict {
    /// The peer speaks what the plane serves; hand the socket on.
    Agrees,
    /// The peer speaks neither transport recognisably (review); hand the
    /// socket on, and let the plane's own handshake or frame reader say
    /// what it says — a wrong-transport diagnosis would be a guess.
    Unjudged,
    /// It does not. Carries the refusal, already printed to stderr — every
    /// server loop swallows per-connection errors, and a refusal nobody sees
    /// is a reset with extra steps.
    Refused(String),
}

/// Judge an accepted socket against the plane it arrived on.
///
/// `plane` and `knob` are what the operator reads: the plane's name and the
/// node configuration key that sets its transport.
pub async fn judge_dialect(
    tcp: &TcpStream,
    plane: &str,
    knob: &str,
    plane_is_tls: bool,
) -> io::Result<DialectVerdict> {
    let dialect = sniff_dialect(tcp).await?;
    match dialect {
        WireDialect::Other => return Ok(DialectVerdict::Unjudged),
        WireDialect::Tls if plane_is_tls => return Ok(DialectVerdict::Agrees),
        WireDialect::Frame if !plane_is_tls => return Ok(DialectVerdict::Agrees),
        WireDialect::Tls | WireDialect::Frame => {}
    }
    let peer = tcp
        .peer_addr()
        .map(|address| address.to_string())
        .unwrap_or_else(|_| "a peer".to_owned());
    let message = cross_mode_refusal(plane, knob, plane_is_tls, &peer);
    // Once per window per plane (review), never per connection: the verdict
    // is not rate-limited — every peer is refused and counted — only the
    // line on stderr is.
    warn_rate_limited(
        &format!(
            "cross-mode:{plane}:{}",
            if plane_is_tls { "tls" } else { "plaintext" }
        ),
        &message,
    );
    Ok(DialectVerdict::Refused(message))
}

/// Warnings that a peer can provoke before it is authenticated are printed
/// at most once per window per key, with a count of what was suppressed
/// (review): a driver retrying a transport mismatch every backoff, or a peer
/// sending one byte in a loop, must not turn a refusal into a log flood that
/// blocks workers on a synchronous stderr.
const WARN_WINDOW: Duration = Duration::from_secs(10);
static WARN_LOG: Mutex<Option<HashMap<String, (Instant, u64)>>> = Mutex::new(None);

/// Print `message` for `key` unless one was printed for it within the
/// window; suppressed repeats are counted and reported with the next line
/// printed. Returns whether it printed.
pub fn warn_rate_limited(key: &str, message: &str) -> bool {
    warn_rate_limited_at(key, message, Instant::now(), WARN_WINDOW)
}

fn warn_rate_limited_at(key: &str, message: &str, now: Instant, window: Duration) -> bool {
    let mut guard = WARN_LOG
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let log = guard.get_or_insert_with(HashMap::new);
    if let Some((last, suppressed)) = log.get_mut(key) {
        if now.saturating_duration_since(*last) < window {
            *suppressed += 1;
            return false;
        }
        let count = *suppressed;
        *last = now;
        *suppressed = 0;
        if count > 0 {
            eprintln!("warning: {message} ({count} more since the last report)");
        } else {
            eprintln!("warning: {message}");
        }
        return true;
    }
    log.insert(key.to_owned(), (now, 0));
    eprintln!("warning: {message}");
    true
}

/// The hint a TLS client adds when its handshake died unanswered (#294 slice
/// 6): a plaintext plane refuses a TLS hello by closing, so the client sees a
/// reset or an end of stream, and only the server's log names the mismatch.
/// Any other handshake failure — a bad certificate, a wrong name, a refused
/// cipher — is left exactly as rustls reported it.
pub fn tls_handshake_hint(error: &io::Error) -> &'static str {
    match error.kind() {
        io::ErrorKind::ConnectionReset
        | io::ErrorKind::ConnectionAborted
        | io::ErrorKind::BrokenPipe
        | io::ErrorKind::UnexpectedEof => {
            " (the peer closed the handshake unanswered, as a plaintext plane does to a TLS \
             client; if that plane is plaintext, dial it with `transport: plaintext`)"
        }
        _ => "",
    }
}

/// [`tls_handshake_hint`] folded into the error itself, for callers whose
/// error type carries an `io::Error` rather than a message.
pub fn with_tls_handshake_hint(error: io::Error) -> io::Error {
    let hint = tls_handshake_hint(&error);
    if hint.is_empty() {
        error
    } else {
        io::Error::new(error.kind(), format!("{error}{hint}"))
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for MaybeTls<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
            Self::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for MaybeTls<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
            Self::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
            Self::Plain(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// The plain variant is a faithful pass-through in both directions.
    ///
    /// Worth pinning rather than assuming: the whole point of the enum is that
    /// callers stop caring which variant they hold, so a plain stream that
    /// silently truncated or reordered would be a bug nothing else would catch
    /// — every caller is generic over the trait and would look correct.
    #[tokio::test]
    async fn the_plain_variant_passes_bytes_through_unchanged() {
        let (client, server) = tokio::io::duplex(64);
        let mut client = MaybeTls::Plain(client);
        let mut server = MaybeTls::Plain(server);

        assert!(!client.is_encrypted());
        assert!(
            client.peer_certificates().is_none(),
            "no TLS means no identity, and the accessor must say so rather than \
             report an empty chain"
        );

        client.write_all(b"VTPM-ish payload").await.unwrap();
        client.flush().await.unwrap();
        let mut got = [0_u8; 16];
        server.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"VTPM-ish payload");

        // And back, because a half-working duplex would still pass the above.
        server.write_all(b"reply").await.unwrap();
        server.flush().await.unwrap();
        let mut back = [0_u8; 5];
        client.read_exact(&mut back).await.unwrap();
        assert_eq!(&back, b"reply");
    }
}

#[cfg(test)]
mod dialect_tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        (client, server)
    }

    #[tokio::test]
    async fn a_tls_hello_and_a_frame_are_told_apart_without_being_consumed() {
        let (mut client, mut server) = pair().await;
        client
            .write_all(&[0x16, 0x03, 0x01, 0x00, 0x05])
            .await
            .unwrap();
        assert_eq!(sniff_dialect(&server).await.unwrap(), WireDialect::Tls);
        let mut first = [0_u8; 5];
        server.read_exact(&mut first).await.unwrap();
        assert_eq!(
            first,
            [0x16, 0x03, 0x01, 0x00, 0x05],
            "peeked, not consumed"
        );

        let (mut client, server) = pair().await;
        client.write_all(b"VTPM").await.unwrap();
        assert_eq!(sniff_dialect(&server).await.unwrap(), WireDialect::Frame);
        let (mut client, server) = pair().await;
        client.write_all(b"VTPW").await.unwrap();
        assert_eq!(sniff_dialect(&server).await.unwrap(), WireDialect::Frame);
        let (mut client, server) = pair().await;
        client.write_all(b"GET / HTTP/1.1\r\n").await.unwrap();
        assert_eq!(
            sniff_dialect(&server).await.unwrap(),
            WireDialect::Other,
            "a probe is neither, and is not called a client on the wrong transport"
        );

        let (client, server) = pair().await;
        drop(client);
        assert_eq!(
            sniff_dialect(&server).await.unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof,
            "a peer that hung up first is a hang-up, not a dialect"
        );
    }

    #[tokio::test]
    async fn a_peer_speaking_the_other_transport_is_refused_naming_both_sides() {
        let (mut client, server) = pair().await;
        client.write_all(&[0x16, 0x03, 0x01]).await.unwrap();
        let DialectVerdict::Refused(message) =
            judge_dialect(&server, "admin", "admin_transport", false)
                .await
                .unwrap()
        else {
            panic!("a TLS hello on a plaintext plane must be refused")
        };
        for named in [
            "opened a TLS handshake",
            "`admin_transport: plaintext`",
            "`transport: plaintext`",
            "`admin_transport: tls`",
        ] {
            assert!(message.contains(named), "{message} must name {named}");
        }

        let (mut client, server) = pair().await;
        client.write_all(b"VTPM").await.unwrap();
        let DialectVerdict::Refused(message) =
            judge_dialect(&server, "peer", "peer_transport", true)
                .await
                .unwrap()
        else {
            panic!("a plaintext frame on a TLS plane must be refused")
        };
        for named in [
            "sent a plaintext frame",
            "`peer_transport: tls`",
            "`transport: tls`",
            "`peer_transport: plaintext`",
        ] {
            assert!(message.contains(named), "{message} must name {named}");
        }

        // Neither transport (review): unjudged on both kinds of plane, so
        // a scan or an HTTP probe never reads as a misconfiguration.
        for plane_is_tls in [true, false] {
            let (mut client, server) = pair().await;
            client.write_all(b"GET / HTTP/1.1\r\n").await.unwrap();
            assert!(
                matches!(
                    judge_dialect(&server, "admin", "admin_transport", plane_is_tls)
                        .await
                        .unwrap(),
                    DialectVerdict::Unjudged
                ),
                "tls plane: {plane_is_tls}"
            );
        }

        // Agreement, both ways, hands the socket on.
        let (mut client, server) = pair().await;
        client.write_all(b"VTPM").await.unwrap();
        assert!(matches!(
            judge_dialect(&server, "peer", "peer_transport", false)
                .await
                .unwrap(),
            DialectVerdict::Agrees
        ));
        let (mut client, server) = pair().await;
        client.write_all(&[0x16]).await.unwrap();
        assert!(matches!(
            judge_dialect(&server, "peer", "peer_transport", true)
                .await
                .unwrap(),
            DialectVerdict::Agrees
        ));
    }

    /// One line per window per key; the count of what was suppressed rides
    /// on the next line printed.
    #[test]
    fn a_repeated_warning_is_printed_once_per_window_with_the_count() {
        let start = Instant::now();
        let window = Duration::from_secs(10);
        let key = "test:rate-limit";
        assert!(warn_rate_limited_at(key, "first", start, window));
        assert!(!warn_rate_limited_at(
            key,
            "second",
            start + Duration::from_secs(1),
            window
        ));
        assert!(!warn_rate_limited_at(
            key,
            "third",
            start + Duration::from_secs(9),
            window
        ));
        assert!(warn_rate_limited_at(
            key,
            "fourth",
            start + Duration::from_secs(11),
            window
        ));
        assert!(
            warn_rate_limited_at(
                "test:other-key",
                "elsewhere",
                start + Duration::from_secs(1),
                window
            ),
            "keys are independent"
        );
    }

    #[test]
    fn only_an_unanswered_handshake_earns_the_plaintext_hint() {
        for kind in [
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::UnexpectedEof,
            io::ErrorKind::ConnectionAborted,
            io::ErrorKind::BrokenPipe,
        ] {
            assert!(
                tls_handshake_hint(&io::Error::from(kind)).contains("`transport: plaintext`"),
                "{kind:?}"
            );
        }
        assert_eq!(
            tls_handshake_hint(&io::Error::new(
                io::ErrorKind::InvalidData,
                "bad certificate"
            )),
            "",
            "a real TLS failure is left as rustls reported it"
        );
        let folded = with_tls_handshake_hint(io::Error::from(io::ErrorKind::ConnectionReset));
        assert_eq!(
            folded.kind(),
            io::ErrorKind::ConnectionReset,
            "the kind survives"
        );
        assert!(
            folded.to_string().contains("`transport: plaintext`"),
            "{folded}"
        );
        let untouched = with_tls_handshake_hint(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad certificate",
        ));
        assert_eq!(untouched.to_string(), "bad certificate");
    }
}
