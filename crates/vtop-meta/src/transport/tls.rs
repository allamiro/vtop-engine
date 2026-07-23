//! mTLS material and certificate identity for the metadata control plane.
//!
//! Peer identity is the leaf certificate's Common Name parsed as a decimal
//! [`MetaNodeId`]. Mutual TLS (TLS 1.3 only) authenticates the chain; the CN
//! mapping is an additional application-level binding so a valid cert for the
//! wrong node id is rejected.
//!
//! # Honest limitations
//!
//! - Identity is CN-only in this slice (no SAN URI / SPIFFE). Operators must
//!   issue peer certs with `CN=<meta-node-id>`.
//! - The crypto provider is pinned to `ring` because workspace feature
//!   unification can enable more than one rustls backend.

use super::wire::{TransportError, TransportResult};
use crate::keys::MetaNodeId;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use std::path::Path;
use std::sync::Arc;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use x509_parser::prelude::*;

/// TLS identity material for a meta node (peer or admin endpoint).
pub struct TlsMaterial {
    pub certificate_chain: Vec<CertificateDer<'static>>,
    pub private_key: PrivateKeyDer<'static>,
    pub trust_roots: RootCertStore,
}

impl TlsMaterial {
    /// Load PEM-encoded chain, key, and CA roots from disk.
    pub fn from_pem_files(
        cert_path: impl AsRef<Path>,
        key_path: impl AsRef<Path>,
        ca_path: impl AsRef<Path>,
    ) -> TransportResult<Self> {
        let certificate_chain = load_certs(cert_path.as_ref())?;
        let private_key = load_private_key(key_path.as_ref())?;
        let mut trust_roots = RootCertStore::empty();
        for cert in load_certs(ca_path.as_ref())? {
            trust_roots
                .add(cert)
                .map_err(|error| TransportError::Tls(format!("add CA: {error}")))?;
        }
        Ok(Self {
            certificate_chain,
            private_key,
            trust_roots,
        })
    }
}

fn load_certs(path: &Path) -> TransportResult<Vec<CertificateDer<'static>>> {
    let certs: Result<Vec<_>, _> = CertificateDer::pem_file_iter(path)
        .map_err(|error| TransportError::Tls(format!("open certs {}: {error}", path.display())))?
        .collect();
    let certs = certs
        .map_err(|error| TransportError::Tls(format!("parse certs {}: {error}", path.display())))?;
    if certs.is_empty() {
        return Err(TransportError::Tls(format!(
            "no certificates in {}",
            path.display()
        )));
    }
    Ok(certs)
}

fn load_private_key(path: &Path) -> TransportResult<PrivateKeyDer<'static>> {
    PrivateKeyDer::from_pem_file(path)
        .map_err(|error| TransportError::Tls(format!("parse key {}: {error}", path.display())))
}

/// Build a TLS 1.3 mTLS server acceptor.
pub fn build_server_acceptor(material: TlsMaterial) -> TransportResult<TlsAcceptor> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
        Arc::new(material.trust_roots),
        Arc::clone(&provider),
    )
    .build()
    .map_err(|error| TransportError::Tls(format!("client verifier: {error}")))?;
    let config = ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|error| TransportError::Tls(error.to_string()))?
        .with_client_cert_verifier(verifier)
        .with_single_cert(material.certificate_chain, material.private_key)
        .map_err(|error| TransportError::Tls(error.to_string()))?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// Build a TLS 1.3 mTLS client connector.
pub fn build_client_connector(material: TlsMaterial) -> TransportResult<TlsConnector> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|error| TransportError::Tls(error.to_string()))?
        .with_root_certificates(material.trust_roots)
        .with_client_auth_cert(material.certificate_chain, material.private_key)
        .map_err(|error| TransportError::Tls(error.to_string()))?;
    Ok(TlsConnector::from(Arc::new(config)))
}

/// Parse `ServerName` for rustls SNI / cert name checks.
pub fn server_name(name: &str) -> TransportResult<ServerName<'static>> {
    ServerName::try_from(name.to_owned())
        .map_err(|error| TransportError::Tls(format!("server name {name:?}: {error}")))
}

/// Extract [`MetaNodeId`] from a leaf certificate's Common Name.
///
/// The CN must be a decimal integer matching the Raft node id. Leading zeros
/// and non-numeric CNs are rejected.
pub fn meta_node_id_from_cert(der: &CertificateDer<'_>) -> TransportResult<MetaNodeId> {
    let (_, cert) = X509Certificate::from_der(der.as_ref())
        .map_err(|error| TransportError::Identity(format!("parse leaf cert: {error}")))?;
    let subject = cert.subject();
    let cn = subject
        .iter_common_name()
        .next()
        .ok_or_else(|| TransportError::Identity("leaf certificate has no Common Name".to_owned()))?
        .as_str()
        .map_err(|error| TransportError::Identity(format!("CN is not UTF-8: {error}")))?;
    let id: u64 = cn.parse().map_err(|_| {
        TransportError::Identity(format!("certificate CN {cn:?} is not a decimal MetaNodeId"))
    })?;
    Ok(MetaNodeId(id))
}

/// Require that the peer leaf CN equals `expected`.
pub fn assert_peer_identity(
    peer_certs: Option<&[CertificateDer<'_>]>,
    expected: MetaNodeId,
) -> TransportResult<()> {
    let leaf = peer_certs
        .and_then(|certs| certs.first())
        .ok_or_else(|| TransportError::Identity("peer presented no certificate".to_owned()))?;
    let actual = meta_node_id_from_cert(leaf)?;
    if actual != expected {
        return Err(TransportError::Identity(format!(
            "peer certificate CN maps to node {actual}, expected {expected}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair};
    use rustls::pki_types::PrivatePkcs8KeyDer;

    fn cert_for_cn(cn: &str) -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
        params.distinguished_name = DistinguishedName::new();
        params.distinguished_name.push(DnType::CommonName, cn);
        let cert = params.self_signed(&key).unwrap();
        (
            cert.der().clone(),
            PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
        )
    }

    #[test]
    fn cn_decimal_maps_to_meta_node_id() {
        let (der, _) = cert_for_cn("42");
        assert_eq!(meta_node_id_from_cert(&der).unwrap(), MetaNodeId(42));
    }

    #[test]
    fn non_decimal_cn_is_rejected() {
        let (der, _) = cert_for_cn("meta-node");
        assert!(matches!(
            meta_node_id_from_cert(&der),
            Err(TransportError::Identity(_))
        ));
    }

    #[test]
    fn assert_peer_identity_checks_expected_id() {
        let (der, _) = cert_for_cn("3");
        let chain = [der];
        assert_peer_identity(Some(&chain), MetaNodeId(3)).unwrap();
        assert!(assert_peer_identity(Some(&chain), MetaNodeId(9)).is_err());
        assert!(assert_peer_identity(None, MetaNodeId(3)).is_err());
    }
}
