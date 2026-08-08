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

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

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
