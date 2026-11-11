//! Pin the display's derived method against each real backend's verification.
use vtop_core::checksum::digest_bytes;
use vtop_core::config::UploadConfig;
use vtop_core::ChecksumAlgorithm;
use vtop_upload::{verification_method, ObjectChecksum, UploadBackend, VerificationMethod};

const ALGORITHMS: [ChecksumAlgorithm; 3] = [
    ChecksumAlgorithm::Sha256,
    ChecksumAlgorithm::Blake3,
    ChecksumAlgorithm::None,
];
async fn assert_read_back(backend: &dyn UploadBackend, uri: &str) {
    for algorithm in ALGORITHMS {
        let digest = digest_bytes(algorithm, b"payload");
        let algorithm_name = algorithm.to_string();
        let expected = digest
            .as_deref()
            .map(|d| ObjectChecksum::new(&algorithm_name, d));
        let result = backend.verify_object(uri, 7, expected).await.unwrap();
        assert!(
            result.passed,
            "{}: {}",
            backend.backend_name(),
            result.message
        );
        assert_eq!(result.backend_limited, algorithm == ChecksumAlgorithm::None);
        assert_eq!(
            verification_method(backend.backend_name(), algorithm),
            if result.backend_limited {
                VerificationMethod::SizeOnly
            } else {
                VerificationMethod::ReadBack
            }
        );
        if algorithm != ChecksumAlgorithm::None {
            let wrong = backend
                .verify_object(
                    uri,
                    7,
                    Some(ObjectChecksum::new(&algorithm_name, &"0".repeat(64))),
                )
                .await
                .unwrap();
            assert!(
                !wrong.passed,
                "{} accepted a wrong digest",
                backend.backend_name()
            );
        }
    }
}
#[tokio::test]
async fn localfs_method_matches_verify_object() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("input");
    std::fs::write(&file, b"payload").unwrap();
    let backend = vtop_upload::localfs_backend::LocalFsBackend::new(dir.path().join("objects"));
    backend.put_object(&file, "s3://b/key", None).await.unwrap();
    assert_read_back(&backend, "s3://b/key").await;
}
#[cfg(unix)]
async fn command_backend(name: &str, version: &str) {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let binary = dir.path().join("tool");
    // Metadata deliberately contains no service digest. A strong pass must
    // hash stdout from the real command verification path.
    std::fs::write(&binary, format!("#!/bin/sh\ncase \"$*\" in\n*--version*) echo '{version}';;\n*head-object*) echo '{{\"ContentLength\":7}}';;\n*stat*) echo '{{\"size\":7}}';;\n*info*) echo 'File size: 7';;\n*) printf payload;;\nesac\n")).unwrap();
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut config: UploadConfig = serde_json::from_value(
        serde_json::json!({"bucket":"b","backend":name,"command_binary":binary}),
    )
    .unwrap();
    config.command_timeout_seconds = 2;
    let backend = vtop_upload::build_backend(&config).await.unwrap();
    assert_read_back(backend.as_ref(), "s3://b/key").await;
}
#[cfg(unix)]
#[tokio::test]
async fn awscli_method_matches_verify_object() {
    command_backend("awscli", "aws-cli/2.0").await;
}
#[cfg(unix)]
#[tokio::test]
async fn minio_method_matches_verify_object() {
    command_backend("minio", "mc version test").await;
}
#[cfg(unix)]
#[tokio::test]
async fn s3cmd_method_matches_verify_object() {
    command_backend("s3cmd", "s3cmd version test").await;
}

#[test]
fn native_s3_method_matches_verify_object() {
    // Credentials belong only to a child process, avoiding process-global env
    // mutation while unrelated tests run.
    if std::env::var_os("VTOP_METHOD_CHILD").is_none() {
        let out = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "native_s3_method_matches_verify_object",
                "--nocapture",
            ])
            .env("VTOP_METHOD_CHILD", "1")
            .env("AWS_ACCESS_KEY_ID", "test")
            .env("AWS_SECRET_ACCESS_KEY", "test")
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        return;
    }
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured = requests.clone();
        let server = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let captured = captured.clone();
                tokio::spawn(async move {
                    let mut request = Vec::new();
                    loop { let mut byte = [0]; if socket.read_exact(&mut byte).await.is_err() { return; } request.push(byte[0]); if request.ends_with(b"\r\n\r\n") { break; } }
                    let head = request.starts_with(b"HEAD ");
                    captured.lock().unwrap().push(if head { "HEAD" } else { "GET" });
                    // base64 SHA-256 of 'payload', computed independently.
                    let checksum = "I59Z7VXnN8dxR89VrQwbAwttfudIp0JpUvm4UtWpNeU=";
                    let response = format!("HTTP/1.1 200 OK\r\nContent-Length: 7\r\nx-amz-checksum-sha256: {checksum}\r\nConnection: close\r\n\r\n{}", if head { "" } else { "payload" });
                    socket.write_all(response.as_bytes()).await.unwrap();
                });
            }
        });
        let mut config: UploadConfig = serde_json::from_value(serde_json::json!({"bucket":"b","endpoint_url":endpoint,"force_path_style":true,"verify_tls":false})).unwrap(); config.backend="s3_native".into();
        let backend = vtop_upload::build_backend(&config).await.unwrap();
        for algorithm in ALGORITHMS {
            requests.lock().unwrap().clear();
            let digest = digest_bytes(algorithm, b"payload"); let name=algorithm.to_string();
            let result = backend.verify_object("s3://b/key", 7, digest.as_deref().map(|d| ObjectChecksum::new(&name,d))).await.unwrap();
            assert!(result.passed, "{}", result.message);
            let method=verification_method("s3_native",algorithm);
            assert_eq!(method, match algorithm { ChecksumAlgorithm::Sha256=>VerificationMethod::StoreChecksum, ChecksumAlgorithm::Blake3=>VerificationMethod::ReadBack, ChecksumAlgorithm::None=>VerificationMethod::SizeOnly });
            assert_eq!(*requests.lock().unwrap(), if method==VerificationMethod::ReadBack {vec!["HEAD","GET"]} else {vec!["HEAD"]});
            assert_eq!(result.backend_limited, method==VerificationMethod::SizeOnly);
        }
        server.abort();
    });
}

#[test]
fn mapping_covers_every_registered_backend_and_algorithm() {
    for backend in [
        "s3_native",
        "localfs",
        "awscli",
        "minio",
        "s3cmd",
        "mock",
        "mock_fail",
        "mock_limited",
    ] {
        for algorithm in ALGORITHMS {
            let method = verification_method(backend, algorithm);
            let expected = match (backend, algorithm) {
                (_, ChecksumAlgorithm::None) | ("mock_limited", _) => "size_only",
                ("s3_native", ChecksumAlgorithm::Sha256) => "store_checksum",
                _ => "read_back",
            };
            assert_eq!(method.as_str(), expected, "{backend}/{algorithm}");
            assert_eq!(serde_json::to_value(method).unwrap(), expected);
        }
    }
}

/// The in-memory backends, keyed by their CONFIG name: `mock_limited` reports
/// `backend_name() == "mock"`, so the method is looked up by the
/// `upload.backend` string, as the display does. (`mock_fail` fails every
/// verification by design, so it has no method to pin.)
#[tokio::test]
async fn mock_backends_method_matches_verify_object() {
    for name in ["mock", "mock_limited"] {
        let config: UploadConfig =
            serde_json::from_value(serde_json::json!({"bucket":"b","backend":name})).unwrap();
        let backend = vtop_upload::build_backend(&config).await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("input");
        std::fs::write(&file, b"payload").unwrap();
        backend.put_object(&file, "s3://b/key", None).await.unwrap();
        for algorithm in ALGORITHMS {
            let digest = digest_bytes(algorithm, b"payload");
            let algorithm_name = algorithm.to_string();
            let expected = digest
                .as_deref()
                .map(|d| ObjectChecksum::new(&algorithm_name, d));
            let result = backend
                .verify_object("s3://b/key", 7, expected)
                .await
                .unwrap();
            assert!(result.passed, "{name}/{algorithm}: {}", result.message);
            let method = verification_method(name, algorithm);
            assert_eq!(
                result.backend_limited,
                method == VerificationMethod::SizeOnly,
                "{name}/{algorithm}: {method:?} but verify_object said {result:?}"
            );
            if method == VerificationMethod::ReadBack {
                let wrong = backend
                    .verify_object(
                        "s3://b/key",
                        7,
                        Some(ObjectChecksum::new(&algorithm_name, &"0".repeat(64))),
                    )
                    .await
                    .unwrap();
                assert!(!wrong.passed, "{name} accepted a wrong {algorithm} digest");
            }
        }
    }
}
