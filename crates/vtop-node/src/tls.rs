//! PEM loading glue: one loader (vtop-meta's) feeding every TLS surface.
//!
//! `ReplicaTlsMaterial` / `ServerTlsMaterial` are in-memory DER structs with
//! no file loaders of their own; rather than duplicate PEM parsing, convert
//! from [`vtop_meta::TlsMaterial`], whose field types match exactly.

use crate::config::TlsPaths;
use std::sync::Arc;
use vtop_broker::replication::ReplicaTlsMaterial;
use vtop_broker::ServerTlsMaterial;
use vtop_meta::TlsMaterial;

pub fn meta_material(paths: &TlsPaths) -> Result<TlsMaterial, String> {
    TlsMaterial::from_pem_files(&paths.cert, &paths.key, &paths.ca)
        .map_err(|error| format!("load TLS material: {error}"))
}

pub fn replica_material(paths: &TlsPaths) -> Result<ReplicaTlsMaterial, String> {
    let material = meta_material(paths)?;
    Ok(ReplicaTlsMaterial {
        certificate_chain: material.certificate_chain,
        private_key: material.private_key,
        trust_roots: material.trust_roots,
    })
}

pub fn server_material(paths: &TlsPaths) -> Result<ServerTlsMaterial, String> {
    let material = meta_material(paths)?;
    Ok(ServerTlsMaterial {
        certificate_chain: material.certificate_chain,
        private_key: material.private_key,
        client_roots: material.trust_roots,
    })
}

/// TLS 1.3 mTLS client config for the produce/fetch and replica planes.
pub fn client_config(paths: &TlsPaths) -> Result<Arc<rustls::ClientConfig>, String> {
    let material = meta_material(paths)?;
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .map_err(|error| error.to_string())?
    .with_root_certificates(material.trust_roots)
    .with_client_auth_cert(material.certificate_chain, material.private_key)
    .map_err(|error| error.to_string())?;
    Ok(Arc::new(config))
}
