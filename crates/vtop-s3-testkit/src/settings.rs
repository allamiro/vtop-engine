//! How a live test reaches the store (#722, change C5): one place reads the
//! environment, so no test hard-codes a region, an addressing style, a TLS
//! flag or an endpoint, and the same binaries run against every server.

use crate::server::Server;

/// `VTOP_TEST_S3_ENDPOINT`: the store's URL. Required for a lab server;
/// optional where the SDK finds the endpoint itself (AWS).
pub const ENDPOINT_ENV: &str = "VTOP_TEST_S3_ENDPOINT";
/// `VTOP_TEST_S3_REGION` (default `us-east-1`).
pub const REGION_ENV: &str = "VTOP_TEST_S3_REGION";
/// `VTOP_TEST_S3_PATH_STYLE` (default `true`: every lab server, D12).
pub const PATH_STYLE_ENV: &str = "VTOP_TEST_S3_PATH_STYLE";
/// `VTOP_TEST_S3_VERIFY_TLS` (default `false`: the lab is plaintext).
pub const VERIFY_TLS_ENV: &str = "VTOP_TEST_S3_VERIFY_TLS";
/// `VTOP_TEST_S3_BUCKET` (default `telemetry-raw`): a bucket without
/// Object Lock.
pub const BUCKET_ENV: &str = "VTOP_TEST_S3_BUCKET";
/// `VTOP_TEST_S3_LOCKED_BUCKET` (default `telemetry-locked`): a bucket
/// created with Object Lock (and so with versioning).
pub const LOCKED_BUCKET_ENV: &str = "VTOP_TEST_S3_LOCKED_BUCKET";

/// What a live test needs to reach the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Settings {
    /// `None` only for a server whose file sets `sdk_default_endpoint`.
    pub endpoint: Option<String>,
    pub region: String,
    pub force_path_style: bool,
    pub verify_tls: bool,
    /// A bucket without Object Lock.
    pub bucket: String,
    /// A bucket created with Object Lock.
    pub locked_bucket: String,
}

impl S3Settings {
    /// Read the settings for `server` from the environment. Panics naming
    /// the variable when one is missing or malformed.
    pub fn from_env(server: &Server) -> S3Settings {
        S3Settings::from_lookup(server, |name| std::env::var(name).ok())
            .unwrap_or_else(|e| panic!("{e}"))
    }

    /// [`from_env`](Self::from_env) over any lookup, so the rules are tested
    /// without touching the process environment.
    pub fn from_lookup(
        server: &Server,
        lookup: impl Fn(&str) -> Option<String>,
    ) -> Result<S3Settings, String> {
        let endpoint = lookup(ENDPOINT_ENV).filter(|v| !v.is_empty());
        if endpoint.is_none() && !server.sdk_default_endpoint {
            return Err(format!(
                "{ENDPOINT_ENV} must name the live endpoint for server {} (only a server whose \
                 capability file sets sdk_default_endpoint: true may leave it unset)",
                server.id
            ));
        }
        let flag = |name: &str, default: bool| -> Result<bool, String> {
            match lookup(name).as_deref() {
                None | Some("") => Ok(default),
                Some("true" | "1") => Ok(true),
                Some("false" | "0") => Ok(false),
                Some(other) => Err(format!("{name}={other:?} is not true or false")),
            }
        };
        let text = |name: &str, default: &str| {
            lookup(name)
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| default.to_owned())
        };
        let verify_tls = flag(VERIFY_TLS_ENV, false)?;
        // The tests' own SDK clients (listing versions, legal holds) verify
        // TLS whatever the flag says, so an https endpoint with the flag off
        // would fail in them, not in VTOP (review of #764). Every lab server
        // is plain http; say so rather than fail later.
        if let Some(endpoint) = endpoint.as_deref().filter(|e| e.starts_with("https://")) {
            if !verify_tls {
                return Err(format!(
                    "{ENDPOINT_ENV}={endpoint} is https with {VERIFY_TLS_ENV}=false: the live \
                     tests' direct SDK clients always verify TLS; use the lab's http endpoint, \
                     or a certificate the host trusts and {VERIFY_TLS_ENV}=true"
                ));
            }
        }
        Ok(S3Settings {
            endpoint,
            region: text(REGION_ENV, "us-east-1"),
            force_path_style: flag(PATH_STYLE_ENV, true)?,
            verify_tls,
            bucket: text(BUCKET_ENV, "telemetry-raw"),
            locked_bucket: text(LOCKED_BUCKET_ENV, "telemetry-locked"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn server(sdk_default_endpoint: bool) -> Server {
        let mut text = crate::server::tests::file_with(true, &[]);
        if sdk_default_endpoint {
            text.push_str("sdk_default_endpoint: true\n");
        }
        Server::parse(&text, Path::new("test.yaml")).unwrap()
    }

    fn lookup<'a>(vars: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |name| {
            vars.iter()
                .find(|(k, _)| *k == name)
                .map(|(_, v)| v.to_string())
        }
    }

    #[test]
    fn the_lab_defaults_hold_with_only_an_endpoint() {
        let s = S3Settings::from_lookup(&server(false), lookup(&[(ENDPOINT_ENV, "http://x:9000")]))
            .unwrap();
        assert_eq!(
            s,
            S3Settings {
                endpoint: Some("http://x:9000".into()),
                region: "us-east-1".into(),
                force_path_style: true,
                verify_tls: false,
                bucket: "telemetry-raw".into(),
                locked_bucket: "telemetry-locked".into(),
            }
        );
    }

    #[test]
    fn a_lab_server_needs_an_endpoint_and_aws_does_not() {
        let err = S3Settings::from_lookup(&server(false), lookup(&[])).unwrap_err();
        assert!(err.contains(ENDPOINT_ENV), "{err}");
        let s = S3Settings::from_lookup(
            &server(true),
            lookup(&[
                (REGION_ENV, "eu-west-1"),
                (PATH_STYLE_ENV, "false"),
                (VERIFY_TLS_ENV, "true"),
                (BUCKET_ENV, "plain"),
                (LOCKED_BUCKET_ENV, "locked"),
            ]),
        )
        .unwrap();
        assert_eq!(s.endpoint, None);
        assert_eq!(s.region, "eu-west-1");
        assert!(!s.force_path_style && s.verify_tls);
        assert_eq!(
            (s.bucket.as_str(), s.locked_bucket.as_str()),
            ("plain", "locked")
        );
    }

    #[test]
    fn an_https_endpoint_without_tls_verification_is_refused() {
        let err = S3Settings::from_lookup(
            &server(false),
            lookup(&[(ENDPOINT_ENV, "https://lab:9000")]),
        )
        .unwrap_err();
        assert!(err.contains("always verify TLS"), "{err}");
        assert!(S3Settings::from_lookup(
            &server(false),
            lookup(&[(ENDPOINT_ENV, "https://lab:9000"), (VERIFY_TLS_ENV, "true")]),
        )
        .is_ok());
    }

    #[test]
    fn a_malformed_flag_is_refused_naming_it() {
        let err = S3Settings::from_lookup(
            &server(false),
            lookup(&[(ENDPOINT_ENV, "http://x"), (PATH_STYLE_ENV, "yes")]),
        )
        .unwrap_err();
        assert!(err.contains("VTOP_TEST_S3_PATH_STYLE=\"yes\""), "{err}");
    }
}
