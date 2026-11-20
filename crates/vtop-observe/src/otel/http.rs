//! The HTTP client both signals are sent with.
//!
//! It is the hyper 1 + hyper-rustls stack the aws-sdk already puts in the
//! binary, so OTLP adds no HTTP or TLS crate. TLS uses rustls's `ring`
//! provider, pinned here as at every other construction site in the
//! workspace, with the system's root certificates (`SSL_CERT_FILE` and
//! `SSL_CERT_DIR` name others). Certificate verification cannot be turned off.

use std::sync::Arc;
use std::time::Duration;

use opentelemetry_http::{Bytes, HttpClient, HttpError, Request, Response};

type Connector = hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>;

/// The hyper client, with each request spawned on the process's runtime.
///
/// The trace exporter runs on its own thread, which has no Tokio runtime,
/// and hyper's client needs one to drive its connections. Spawning onto the
/// runtime captured at start-up and awaiting the join handle (which any
/// executor can await) keeps the request on Tokio and the export thread
/// blocked only on the result.
#[derive(Debug, Clone)]
pub(crate) struct OnRuntime {
    client: opentelemetry_http::hyper::HyperClient<Connector>,
    runtime: tokio::runtime::Handle,
}

impl OnRuntime {
    pub(crate) fn new(timeout: Duration, runtime: tokio::runtime::Handle) -> Result<Self, String> {
        use hyper_rustls::ConfigBuilderExt;
        let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?;
        // No system roots is not fatal: a plain http:// collector (the usual
        // in-cluster or sidecar shape) needs none, and an https:// one then
        // fails its exports with a certificate error that says why.
        let tls = match builder.clone().with_native_roots() {
            Ok(builder) => builder.with_no_client_auth(),
            Err(_) => builder
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth(),
        };
        let connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(tls)
            .https_or_http()
            .enable_http1()
            .build();
        Ok(Self {
            client: opentelemetry_http::hyper::HyperClient::new(connector, timeout, None),
            runtime,
        })
    }

    /// POST an OTLP protobuf body. `Err` carries a message fit for a log
    /// line: the status, or the transport error with its URLs redacted.
    pub(crate) async fn post(
        &self,
        url: &str,
        headers: &[(String, String)],
        body: Vec<u8>,
    ) -> Result<(), String> {
        let mut request = Request::builder()
            .method("POST")
            .uri(url)
            .header("content-type", "application/x-protobuf");
        for (name, value) in headers {
            request = request.header(name.as_str(), value.as_str());
        }
        let request = request
            .body(Bytes::from(body))
            .map_err(|_| "the request could not be built (check the endpoint and headers)")?;
        match self.send_bytes(request).await {
            Ok(response) if response.status().is_success() => Ok(()),
            Ok(response) => Err(format!(
                "the collector answered HTTP {}",
                response.status().as_u16()
            )),
            Err(e) => Err(super::redact::redact(&e.to_string())),
        }
    }
}

#[async_trait::async_trait]
impl HttpClient for OnRuntime {
    async fn send_bytes(&self, request: Request<Bytes>) -> Result<Response<Bytes>, HttpError> {
        let client = self.client.clone();
        self.runtime
            .spawn(async move { client.send_bytes(request).await })
            .await
            .map_err(|e| Box::new(e) as HttpError)?
    }
}
