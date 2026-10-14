//! Opening a Postgres store is network work: the pool connects and the schema
//! check queries. A server that accepts the TCP connection and then says
//! nothing used to hang startup outside the bound the deadline wrapper applies
//! to later requests (review of #568).
//!
//! Needs no database: the "server" is a socket that accepts and stays silent.
//! Build with `--features postgres`.

use std::time::Duration;
use tokio::net::TcpListener;
use vtop_state::connect_state_store_with_deadline;

#[tokio::test]
async fn a_silent_postgres_server_fails_the_connect_instead_of_hanging_it() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    // Accept and hold every connection open, answering nothing.
    let held = tokio::spawn(async move {
        let mut sockets = Vec::new();
        while let Ok((socket, _)) = listener.accept().await {
            sockets.push(socket);
        }
    });

    let url = format!("postgres://vtop:vtop@127.0.0.1:{port}/vtop?sslmode=disable");
    let limit = Duration::from_millis(400);
    // The outer bound makes a regression fail the test, not hang it.
    let started = std::time::Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        connect_state_store_with_deadline(&url, limit),
    )
    .await
    .expect("the connect must give up within its own bound, not hang");
    let elapsed = started.elapsed();
    // Not just "did not hang": a connect that ignored `limit` and gave up on
    // some longer timer of its own would pass the outer bound (review of
    // #568). The slack is for a loaded CI runner, and is well under the
    // shortest other timer on the path (the 10 s connect timeout).
    assert!(
        elapsed >= limit && elapsed < limit + Duration::from_millis(1600),
        "gave up after {elapsed:?}; the deadline was {limit:?}"
    );
    let message = match outcome {
        Ok(_) => panic!("a server that never answers cannot have produced a store"),
        Err(error) => error.to_string(),
    };
    assert!(
        message.contains("did not complete within"),
        "the failure must name the deadline, not some other error: {message}"
    );
    held.abort();
}
