//! `vtopctl` binary entry point.

use clap::Parser;
use vtop_cli::commands::{dispatch, Cli};

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let code = dispatch(cli).await;
    std::process::exit(code);
}
