//! PEM loading glue: one loader (vtop-meta's) feeding every TLS surface.
//!
//! `ReplicaTlsMaterial` / `ServerTlsMaterial` are in-memory DER structs with
//! no file loaders of their own; rather than duplicate PEM parsing, convert
//! from [`vtop_meta::TlsMaterial`], whose field types match exactly.

use crate::config::TlsPaths;
use std::path::Path;
use std::sync::Arc;
use vtop_broker::replication::ReplicaTlsMaterial;
use vtop_broker::ServerTlsMaterial;
use vtop_meta::TlsMaterial;

pub fn meta_material(paths: &TlsPaths) -> Result<TlsMaterial, String> {
    check_private_key(&paths.key)?;
    TlsMaterial::from_pem_files(&paths.cert, &paths.key, &paths.ca)
        .map_err(|error| format!("load TLS material: {error}"))
}

/// Refuse a TLS private key other local users can reach, before it is read
/// (#732, S1), by the rule the engine applies to its own secret files
/// (#733): writable by group or other is refused outright; readable by
/// group or other is refused unless the directory is closed to them or the
/// file is a read-only Kubernetes Secret mount (the chart's `0440` with
/// `fsGroup`). A node reads no engine config, so there is no override. The
/// message names the file, its mode and the `chmod` to run. Unix only.
pub fn check_private_key(path: &Path) -> Result<(), String> {
    use vtop_core::secret_file::{enforce, SecretFile, SecretFileKind, SecretPolicy};
    enforce(
        &SecretFile::new(SecretFileKind::TlsPrivateKey, "TLS private key", path),
        &SecretPolicy::strict(),
    )
    .map_err(|refusal| refusal.to_string())
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

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    /// A key in a directory other users can enter, with `mode`. The content
    /// is not a key: a refusal must come before anything reads it.
    fn key_with_mode(dir: &tempfile::TempDir, mode: u32) -> TlsPaths {
        let open = dir.path().join("certs");
        std::fs::create_dir(&open).unwrap();
        std::fs::set_permissions(&open, std::fs::Permissions::from_mode(0o755)).unwrap();
        let key = open.join("node-key.pem");
        std::fs::write(&key, b"not a key").unwrap();
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(mode)).unwrap();
        TlsPaths {
            ca: open.join("ca.pem"),
            cert: open.join("node.pem"),
            key,
        }
    }

    /// #732 (S1): a node refuses a TLS private key other users can read,
    /// naming the file, its mode and the chmod, before reading it.
    #[test]
    fn a_tls_private_key_other_users_can_read_is_refused_before_it_is_read() {
        let dir = tempfile::tempdir().unwrap();
        let paths = key_with_mode(&dir, 0o644);
        let refused = meta_material(&paths).err().expect("a 0644 key is refused");
        let path = paths.key.display().to_string();
        for needed in [
            format!("TLS private key: {path} has mode 0644"),
            "every other user on this host".to_owned(),
            format!("`chmod 0400 {path}`"),
            "the file was not read".to_owned(),
        ] {
            assert!(
                refused.contains(&needed),
                "{needed:?} missing from {refused}"
            );
        }
        assert!(
            !refused.contains("allow_loose_secret_permissions"),
            "a node has no engine config, so no override is offered: {refused}"
        );
        // The observability endpoint's key is held to the same rule.
        assert!(check_private_key(&paths.key).is_err());
    }

    #[test]
    fn a_tls_private_key_other_users_can_write_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let paths = key_with_mode(&dir, 0o660);
        let refused = meta_material(&paths).err().expect("a 0660 key is refused");
        assert!(
            refused.contains("has mode 0660") && refused.contains("`chmod go-w"),
            "{refused}"
        );
    }

    /// An owner-only key passes the check and is then read: the error is
    /// the PEM loader's, not a permission refusal.
    #[test]
    fn an_owner_only_tls_private_key_is_read() {
        let dir = tempfile::tempdir().unwrap();
        let paths = key_with_mode(&dir, 0o600);
        assert!(check_private_key(&paths.key).is_ok());
        let error = meta_material(&paths).err().expect("the files are not PEM");
        assert!(error.starts_with("load TLS material"), "{error}");
    }
}
