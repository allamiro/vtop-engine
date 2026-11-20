//! Who may open a native produce/fetch session on this node.
//!
//! The native plane is mutual TLS: rustls has already checked the client's
//! certificate chain against `native_tls.ca` by the time the hello reaches
//! the authorizer. A valid chain only says the CA issued the certificate to
//! SOMEONE. This node serves exactly one principal (`principal_id`), so the
//! authorizer also asks whether the certificate was issued to THAT principal:
//! the leaf's Common Name must be the principal's UUID, the same field the
//! metadata, replica and admin planes read as an identity. Without that
//! binding, any holder of any CA-signed client certificate could act as the
//! principal by declaring its UUID, which is not a secret.

use rustls::pki_types::CertificateDer;
use uuid::Uuid;
use vtop_broker::SessionAuthorizer;
use vtop_protocol::Role;

/// Admits the one configured principal, as a producer or consumer, to a
/// client whose verified certificate was issued to that principal.
pub(crate) struct PrincipalAuthorizer {
    /// This node, named in the audit record of a refusal.
    node: Uuid,
    /// The one principal this node serves: its `principal_id`, and the
    /// Common Name a client certificate must carry.
    principal: Uuid,
}

/// Why a session was refused, for the audit record and the operator.
#[derive(Debug)]
struct Refusal {
    /// The Common Name the client's certificate carries, when it has a
    /// readable one: the audit record's actor.
    presented: Option<String>,
    /// What happened, what the node did, and what to do.
    reason: String,
}

impl PrincipalAuthorizer {
    pub(crate) fn new(node: Uuid, principal: Uuid) -> Self {
        Self { node, principal }
    }

    /// The principal and role a session may ask for, whatever the transport.
    fn check_declaration(&self, declared: Uuid, role: Role) -> Result<(), String> {
        if !matches!(role, Role::Producer | Role::Consumer) {
            return Err(format!(
                "the client asked for the {role:?} role, which the native plane does not serve. The \
                 node refused the session. Ask for the producer or consumer role."
            ));
        }
        if declared != self.principal {
            return Err(format!(
                "the client asked to act as principal {declared}, but this node serves only {served}. \
                 The node refused the session. Point the client at the principal_id this node is \
                 configured with, or at the node that serves {declared}.",
                served = self.principal
            ));
        }
        Ok(())
    }

    /// The decision for a session over TLS: the declared principal and role,
    /// then the certificate. `chain` is the verified chain, leaf first.
    fn judge(&self, chain: &[Vec<u8>], declared: Uuid, role: Role) -> Result<(), Refusal> {
        let served = self.principal;
        let leaf = chain.first();
        // Read the certificate first even when the declaration is already
        // wrong, so the audit record names who asked.
        let presented = leaf.map(|der| common_name(der));
        let presented_name = presented
            .as_ref()
            .and_then(|read| read.as_ref().ok())
            .cloned();
        let refuse = |reason: String| Refusal {
            presented: presented_name.clone(),
            reason,
        };
        self.check_declaration(declared, role).map_err(refuse)?;
        let Some(read) = presented else {
            return Err(refuse(format!(
                "the session presented no client certificate. The node refused it. A native client \
                 must present a certificate that chains to native_tls.ca and carries \
                 CN={served}, this node's principal_id."
            )));
        };
        let cn = read.map_err(|problem| {
            refuse(format!(
                "the client certificate has no readable Common Name ({problem}). The node refused \
                 the session. Reissue the client certificate with CN={served}, this node's \
                 principal_id."
            ))
        })?;
        let bound = Uuid::parse_str(&cn).map_err(|_| {
            refuse(format!(
                "the client certificate's Common Name {cn:?} is not a UUID, so it is not bound to \
                 any principal. The node refused the session. Reissue the client certificate with \
                 CN={served}, this node's principal_id."
            ))
        })?;
        if bound != served {
            return Err(refuse(format!(
                "the client certificate chains to the trusted CA but was issued to {bound}, not to \
                 principal {served}, so it may not act as {served}. The node refused the session. \
                 Use the certificate issued for principal {served} (CN={served}), or reissue this \
                 client's certificate for the principal it is meant to act as."
            )));
        }
        Ok(())
    }
}

/// The leaf certificate's Common Name, read the way the replica and
/// metadata planes read it.
fn common_name(der: &[u8]) -> Result<String, String> {
    vtop_meta::transport::tls::common_name_from_cert(&CertificateDer::from(der))
        .map_err(|error| error.to_string())
}

fn role_name(role: Role) -> &'static str {
    match role {
        Role::Producer => "producer",
        Role::Consumer => "consumer",
        Role::Peer => "peer",
        Role::Administrator => "administrator",
    }
}

impl SessionAuthorizer for PrincipalAuthorizer {
    fn authorize(&self, peer_chain_der: &[Vec<u8>], principal_id: Uuid, role: Role) -> bool {
        match self.judge(peer_chain_der, principal_id, role) {
            Ok(()) => true,
            Err(refusal) => {
                crate::audit::native_session_refused(
                    self.node,
                    refusal.presented.as_deref(),
                    principal_id,
                    self.principal,
                    role_name(role),
                    &refusal.reason,
                );
                false
            }
        }
    }

    /// On a plaintext native plane (#294) the rule is the same declaration
    /// check — the configured principal, as a producer or consumer — and
    /// the evidence is only the declaration, because there is no
    /// certificate to bind it to. That is the mode's stated cost: the plane
    /// is served only where `native_transport` allowed it to bind.
    fn authorize_unverified(&self, principal_id: Uuid, role: Role) -> bool {
        match self.check_declaration(principal_id, role) {
            Ok(()) => true,
            Err(reason) => {
                crate::audit::native_session_refused(
                    self.node,
                    None,
                    principal_id,
                    self.principal,
                    role_name(role),
                    &reason,
                );
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TlsPaths;
    use crate::tls;
    use rcgen::{
        BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose,
        IsCa, Issuer, KeyPair,
    };
    use std::net::SocketAddr;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::net::{TcpListener, TcpStream};
    use tokio_rustls::TlsConnector;
    use vtop_broker::{
        LocalBroker, NativeServer, ProducerEpochJournal, SegmentFormat, ServerConfig,
        ServerTlsMaterial,
    };
    use vtop_log::{ActiveSegment, KeyRange, RangeLineage, SegmentConfig, SegmentDescriptor};
    use vtop_protocol::{
        read_frame, write_frame, ClientHello, ErrorCode, Message, ProtocolLimits, RangeIdentity,
        WireFrame, PROTOCOL_MAJOR,
    };

    /// The principal this node serves, and another principal the same CA
    /// has issued a certificate to.
    const SERVED: Uuid = Uuid::from_u128(0xB0B);
    const OTHER: Uuid = Uuid::from_u128(0xA11CE);
    const NODE: Uuid = Uuid::from_u128(0xA1);
    const CLUSTER: Uuid = Uuid::from_u128(30);
    const LIMITS: ProtocolLimits = ProtocolLimits {
        max_frame_bytes: 16 * 1024,
        max_records: 32,
    };

    /// A certificate authority that signs the leaves a test needs.
    struct Ca {
        pem: String,
        der: rustls::pki_types::CertificateDer<'static>,
        issuer: Issuer<'static, KeyPair>,
    }

    struct Leaf {
        cert_pem: String,
        key_pem: String,
        der: Vec<u8>,
    }

    fn ca(name: &str) -> Ca {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.distinguished_name = DistinguishedName::new();
        params.distinguished_name.push(DnType::CommonName, name);
        let cert = params.self_signed(&key).unwrap();
        Ca {
            pem: cert.pem(),
            der: cert.der().clone(),
            issuer: Issuer::new(params, key),
        }
    }

    /// A leaf with Common Name `cn`, valid for `dns`, signed by `ca`.
    fn leaf(ca: &Ca, cn: &str, dns: &str) -> Leaf {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec![dns.to_owned()]).unwrap();
        params.distinguished_name = DistinguishedName::new();
        params.distinguished_name.push(DnType::CommonName, cn);
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let cert = params.signed_by(&key, &ca.issuer).unwrap();
        Leaf {
            cert_pem: cert.pem(),
            key_pem: key.serialize_pem(),
            der: cert.der().as_ref().to_vec(),
        }
    }

    /// PEM files in the layout production loads (`native_tls`), so the tests
    /// go through the same loaders a node and a client use.
    fn paths(dir: &Path, name: &str, trusted: &Ca, own: &Leaf) -> TlsPaths {
        let paths = TlsPaths {
            ca: dir.join(format!("{name}-ca.pem")),
            cert: dir.join(format!("{name}.pem")),
            key: dir.join(format!("{name}-key.pem")),
        };
        std::fs::write(&paths.ca, &trusted.pem).unwrap();
        std::fs::write(&paths.cert, &own.cert_pem).unwrap();
        std::fs::write(&paths.key, &own.key_pem).unwrap();
        paths
    }

    fn broker(dir: &Path) -> Arc<LocalBroker> {
        let range_id = Uuid::from_u128(10);
        let range = RangeIdentity {
            topic: "native".to_owned(),
            topic_epoch: 1,
            range_id,
            range_generation: 0,
        };
        let descriptor = SegmentDescriptor {
            segment_id: Uuid::from_u128(11),
            topic: range.topic.clone(),
            topic_epoch: range.topic_epoch,
            lineage: RangeLineage {
                range_id,
                generation: 0,
                key_range: KeyRange::full(),
                parents: Vec::new(),
            },
            base_offset: 0,
        };
        let segment = ActiveSegment::create(
            dir.join("native.active"),
            descriptor,
            SegmentConfig::default(),
        )
        .unwrap();
        let epochs = ProducerEpochJournal::open(dir.join("native.epochs")).unwrap();
        Arc::new(LocalBroker::new(segment, epochs, range, 7).unwrap())
    }

    /// A native plane serving `SERVED` with the production authorizer and
    /// the production TLS loader, trusting `client_ca` and presenting
    /// `server_leaf`.
    struct Plane {
        address: SocketAddr,
        shutdown: Option<tokio::sync::oneshot::Sender<()>>,
        task: Option<tokio::task::JoinHandle<()>>,
        _dir: tempfile::TempDir,
    }

    impl Plane {
        async fn start(client_ca: &Ca, server_leaf: &Leaf) -> Plane {
            let dir = tempfile::tempdir().unwrap();
            let server_paths = paths(dir.path(), "server", client_ca, server_leaf);
            let material: ServerTlsMaterial = tls::server_material(&server_paths).unwrap();
            let server = NativeServer::new(
                broker(dir.path()),
                material,
                Arc::new(PrincipalAuthorizer::new(NODE, SERVED)),
                ServerConfig {
                    cluster_id: CLUSTER,
                    node_id: NODE,
                    segment_format: SegmentFormat::V1,
                    max_frame_bytes: LIMITS.max_frame_bytes,
                    max_records_per_frame: LIMITS.max_records,
                    window_bytes: u64::from(LIMITS.max_frame_bytes),
                    max_sessions: 4,
                    max_inflight_requests: 2,
                    handshake_timeout: Duration::from_secs(2),
                    idle_timeout: Duration::from_secs(2),
                },
            )
            .unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (shutdown, rx) = tokio::sync::oneshot::channel();
            let task = tokio::spawn(async move {
                let _ = server.serve(listener, rx).await;
            });
            Plane {
                address,
                shutdown: Some(shutdown),
                task: Some(task),
                _dir: dir,
            }
        }

        async fn stop(mut self) {
            if let Some(shutdown) = self.shutdown.take() {
                let _ = shutdown.send(());
            }
            if let Some(task) = self.task.take() {
                let _ = task.await;
            }
        }
    }

    /// What a client sees of one attempt to open a session: the node's
    /// reply to the hello, or where the attempt died.
    #[derive(Debug)]
    enum Attempt {
        Admitted,
        Refused(ErrorCode, String),
        /// The TLS handshake or the connection failed, before any reply.
        Failed(String),
    }

    async fn attempt(connector: &TlsConnector, address: SocketAddr, claimed: Uuid) -> Attempt {
        let socket = TcpStream::connect(address).await.unwrap();
        let mut stream = match connector
            .connect(
                rustls::pki_types::ServerName::try_from("localhost").unwrap(),
                socket,
            )
            .await
        {
            Ok(stream) => stream,
            Err(error) => return Attempt::Failed(format!("tls: {error}")),
        };
        let hello = WireFrame {
            request_id: 0,
            stream_id: 0,
            message: Message::ClientHello(ClientHello {
                cluster_id: CLUSTER,
                principal_id: claimed,
                role: Role::Producer,
                minimum_major: PROTOCOL_MAJOR,
                maximum_major: PROTOCOL_MAJOR,
                requested_max_frame_bytes: LIMITS.max_frame_bytes,
                requested_max_records: LIMITS.max_records,
                requested_max_inflight_requests: 1,
                initial_window_bytes: u64::from(LIMITS.max_frame_bytes),
                session_nonce: [7; 32],
            }),
        };
        // Under TLS 1.3 a client whose certificate the server will refuse
        // finishes its own handshake first; the refusal arrives as an alert
        // on the next read or write, so every step can be where it dies.
        if let Err(error) = write_frame(&mut stream, &hello, LIMITS).await {
            return Attempt::Failed(format!("write: {error}"));
        }
        match read_frame(&mut stream, LIMITS).await {
            Ok(Some(WireFrame {
                message: Message::ServerHello(_),
                ..
            })) => Attempt::Admitted,
            Ok(Some(WireFrame {
                message: Message::Error(error),
                ..
            })) => Attempt::Refused(error.code, error.message),
            Ok(Some(other)) => Attempt::Failed(format!("unexpected frame {other:?}")),
            Ok(None) => Attempt::Failed("connection closed before a reply".to_owned()),
            Err(error) => Attempt::Failed(format!("read: {error}")),
        }
    }

    fn connector_with(paths: &TlsPaths) -> TlsConnector {
        TlsConnector::from(tls::client_config(paths).unwrap())
    }

    /// A client that trusts `server_ca` and presents no certificate at all.
    fn connector_without_certificate(server_ca: &Ca) -> TlsConnector {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(server_ca.der.clone()).unwrap();
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        TlsConnector::from(Arc::new(config))
    }

    fn client_files(dir: &Path, name: &str, server_ca: &Ca, own: &Leaf) -> TlsConnector {
        connector_with(&paths(dir, name, server_ca, own))
    }

    // ---- the bug: a certificate for one principal acting as another ----

    /// A client whose certificate the CA issued to principal A, claiming
    /// principal B's UUID, is refused. Before the certificate was bound to
    /// the principal, every CA-signed certificate passed on the UUID alone.
    #[tokio::test]
    async fn a_certificate_issued_to_another_principal_is_refused() {
        let ca = ca("native-ca");
        let plane = Plane::start(&ca, &leaf(&ca, &NODE.to_string(), "localhost")).await;
        let dir = tempfile::tempdir().unwrap();
        let other = leaf(&ca, &OTHER.to_string(), "client");
        let connector = client_files(dir.path(), "other", &ca, &other);

        match attempt(&connector, plane.address, SERVED).await {
            Attempt::Refused(ErrorCode::Unauthorized, message) => {
                assert!(message.contains("not authorized"), "{message}");
            }
            other => panic!("a certificate issued to another principal must be refused: {other:?}"),
        }
        plane.stop().await;
    }

    /// The certificate issued to the principal is accepted.
    #[tokio::test]
    async fn the_certificate_issued_to_the_principal_is_accepted() {
        let ca = ca("native-ca");
        let plane = Plane::start(&ca, &leaf(&ca, &NODE.to_string(), "localhost")).await;
        let dir = tempfile::tempdir().unwrap();
        let own = leaf(&ca, &SERVED.to_string(), "client");
        let connector = client_files(dir.path(), "served", &ca, &own);

        match attempt(&connector, plane.address, SERVED).await {
            Attempt::Admitted => {}
            other => panic!("the principal's own certificate must be admitted: {other:?}"),
        }
        // The right certificate does not let a client act as someone else.
        match attempt(&connector, plane.address, OTHER).await {
            Attempt::Refused(ErrorCode::Unauthorized, _) => {}
            other => panic!("a principal this node does not serve must be refused: {other:?}"),
        }
        plane.stop().await;
    }

    // ---- the plane's other TLS controls (the threat map's N1 and N8) ----

    #[tokio::test]
    async fn an_absent_client_certificate_is_refused() {
        let ca = ca("native-ca");
        let plane = Plane::start(&ca, &leaf(&ca, &NODE.to_string(), "localhost")).await;
        let connector = connector_without_certificate(&ca);

        match attempt(&connector, plane.address, SERVED).await {
            Attempt::Failed(_) => {}
            other => panic!("a client with no certificate must not get a session: {other:?}"),
        }
        plane.stop().await;
    }

    /// A certificate with the right Common Name from a CA the node does not
    /// trust is refused by the handshake: the name alone proves nothing.
    #[tokio::test]
    async fn an_untrusted_client_certificate_is_refused() {
        let ca = ca("native-ca");
        let stranger = self::ca("some-other-ca");
        let plane = Plane::start(&ca, &leaf(&ca, &NODE.to_string(), "localhost")).await;
        let dir = tempfile::tempdir().unwrap();
        let forged = leaf(&stranger, &SERVED.to_string(), "client");
        // The client trusts the node's CA, so only its own certificate is wrong.
        let connector = client_files(dir.path(), "forged", &ca, &forged);

        match attempt(&connector, plane.address, SERVED).await {
            Attempt::Failed(_) => {}
            other => panic!("a certificate from an untrusted CA must not get a session: {other:?}"),
        }
        plane.stop().await;
    }

    /// The client authenticates the node: a server certificate from a CA the
    /// client does not trust, or one that is not valid for the name dialled,
    /// is refused before the client sends its hello.
    #[tokio::test]
    async fn a_client_verifies_the_servers_certificate() {
        let ca = ca("native-ca");
        let stranger = self::ca("some-other-ca");
        let dir = tempfile::tempdir().unwrap();
        let own = leaf(&ca, &SERVED.to_string(), "client");

        // The node presents a certificate the client's CA never issued.
        let imposter = Plane::start(&ca, &leaf(&stranger, &NODE.to_string(), "localhost")).await;
        match attempt(
            &client_files(dir.path(), "a", &ca, &own),
            imposter.address,
            SERVED,
        )
        .await
        {
            Attempt::Failed(error) => assert!(error.starts_with("tls:"), "{error}"),
            other => panic!("an untrusted server certificate must be refused: {other:?}"),
        }
        imposter.stop().await;

        // The node presents a certificate from the right CA for another name.
        let misnamed = Plane::start(&ca, &leaf(&ca, &NODE.to_string(), "elsewhere.example")).await;
        match attempt(
            &client_files(dir.path(), "b", &ca, &own),
            misnamed.address,
            SERVED,
        )
        .await
        {
            Attempt::Failed(error) => assert!(error.starts_with("tls:"), "{error}"),
            other => panic!("a certificate for another name must be refused: {other:?}"),
        }
        misnamed.stop().await;

        // Control: the same client is admitted by a node that is who it says.
        let genuine = Plane::start(&ca, &leaf(&ca, &NODE.to_string(), "localhost")).await;
        match attempt(
            &client_files(dir.path(), "c", &ca, &own),
            genuine.address,
            SERVED,
        )
        .await
        {
            Attempt::Admitted => {}
            other => panic!("the control must be admitted: {other:?}"),
        }
        genuine.stop().await;
    }

    // ---- the authorizer's decision and its audit record ----

    fn authorizer() -> PrincipalAuthorizer {
        PrincipalAuthorizer::new(NODE, SERVED)
    }

    #[test]
    fn a_refused_certificate_is_audited_with_what_happened_and_what_to_do() {
        let ca = ca("native-ca");
        let other = leaf(&ca, &OTHER.to_string(), "client");
        let refusal = authorizer()
            .judge(&[other.der], SERVED, Role::Producer)
            .expect_err("a certificate issued to another principal is refused");
        let event = crate::audit::native_session_refusal(
            NODE,
            refusal.presented.as_deref(),
            SERVED,
            SERVED,
            role_name(Role::Producer),
            &refusal.reason,
        );
        let line = event.to_json();
        assert!(line.contains("\"action\":\"native.session\""), "{line}");
        assert!(line.contains("\"outcome\":\"refused\""), "{line}");
        // Who: the certificate's identity; what they asked for: the principal.
        assert_eq!(
            event.actor.name.as_deref(),
            Some(OTHER.to_string().as_str())
        );
        assert_eq!(event.actor.source, "certificate");
        assert_eq!(
            event.target.id.as_deref(),
            Some(SERVED.to_string().as_str())
        );
        // The three parts: what happened, what the node did, what to do.
        let reason = event.reason.as_deref().unwrap();
        assert!(reason.contains("was issued to"), "{reason}");
        assert!(reason.contains("The node refused the session."), "{reason}");
        assert!(
            reason.contains("Use the certificate issued for principal"),
            "{reason}"
        );
        // The certificate itself is never in the record.
        assert!(!line.contains("BEGIN CERTIFICATE"), "{line}");
    }

    /// A refusal with no certificate to read is attributed to no one.
    #[test]
    fn a_session_with_no_certificate_is_recorded_as_anonymous() {
        let refusal = authorizer()
            .judge(&[], SERVED, Role::Producer)
            .expect_err("no certificate, no session");
        assert!(refusal.presented.is_none());
        let event = crate::audit::native_session_refusal(
            NODE,
            refusal.presented.as_deref(),
            SERVED,
            SERVED,
            "producer",
            &refusal.reason,
        );
        assert_eq!(event.actor.source, "none");
        assert!(event
            .reason
            .unwrap()
            .contains("presented no client certificate"));
    }

    #[test]
    fn the_certificates_that_are_not_bound_to_the_principal_are_all_refused() {
        let ca = ca("native-ca");
        let named_for_a_host = leaf(&ca, "vtop-harness-client", "client");
        let right = leaf(&ca, &SERVED.to_string(), "client");
        let authorizer = authorizer();
        // A CA-signed certificate whose name is not a UUID is bound to no one.
        assert!(!authorizer.authorize(&[named_for_a_host.der], SERVED, Role::Producer));
        // No certificate in the chain, and bytes that are not a certificate.
        assert!(!authorizer.authorize(&[], SERVED, Role::Producer));
        assert!(!authorizer.authorize(&[b"not a certificate".to_vec()], SERVED, Role::Producer));
        // The bound certificate, for the roles the plane serves, and no other.
        assert!(authorizer.authorize(std::slice::from_ref(&right.der), SERVED, Role::Producer));
        assert!(authorizer.authorize(std::slice::from_ref(&right.der), SERVED, Role::Consumer));
        assert!(!authorizer.authorize(std::slice::from_ref(&right.der), SERVED, Role::Peer));
        assert!(!authorizer.authorize(&[right.der], SERVED, Role::Administrator));
    }

    /// A plaintext plane has no certificate to bind: it keeps admitting the
    /// configured principal on its declaration, and nothing else.
    #[test]
    fn a_plaintext_session_is_admitted_on_the_declaration_the_mode_documents() {
        let authorizer = authorizer();
        assert!(authorizer.authorize_unverified(SERVED, Role::Producer));
        assert!(!authorizer.authorize_unverified(OTHER, Role::Producer));
        assert!(!authorizer.authorize_unverified(SERVED, Role::Administrator));
    }
}
