use std::{net::SocketAddr, path::PathBuf};

use clap::{Parser, Subcommand};
use kepos_codex_bridge::{Bridge, bind_loopback, validate_private_auth_file};
use nanocodex_oai_api::{
    Model, OpenAi,
    auth::{ChatGptLogin, load_chatgpt_auth},
};

#[derive(Parser)]
#[command(
    name = "kepos-codex-bridge",
    about = "Expose native Codex Responses through a Kepos loopback service"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the loopback bridge with the selected private Codex auth.json.
    Serve {
        /// Owner-only Codex auth.json. Also read from KEPOS_CODEX_AUTH_FILE.
        #[arg(long, env = "KEPOS_CODEX_AUTH_FILE")]
        auth_file: PathBuf,
        /// Loopback port; 0 selects an ephemeral port for local testing.
        #[arg(long, default_value_t = 8787)]
        port: u16,
        /// Supported Codex model fixed for this bridge process.
        #[arg(long, default_value = "gpt-5.6-luna")]
        model: Model,
        /// Developer instruction used when a request omits instructions.
        #[arg(
            long,
            default_value = "Answer through the native Codex Responses protocol."
        )]
        instructions: String,
    },
    /// Complete the browser-based ChatGPT PKCE login on this host.
    Login {
        /// Destination auth.json. Also read from KEPOS_CODEX_AUTH_FILE.
        #[arg(long, env = "KEPOS_CODEX_AUTH_FILE")]
        auth_file: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_target(false).init();
    match Cli::parse().command {
        Command::Serve {
            auth_file,
            port,
            model,
            instructions,
        } => serve(auth_file, port, model, instructions).await?,
        Command::Login { auth_file } => login(auth_file).await?,
    }
    Ok(())
}

async fn serve(
    auth_file: PathBuf,
    port: u16,
    model: Model,
    instructions: String,
) -> Result<(), Box<dyn std::error::Error>> {
    validate_private_auth_file(&auth_file)?;
    let auth = load_chatgpt_auth(&auth_file)?;
    let openai = OpenAi::builder(auth)
        .model(model)
        .websocket_warmup(false)
        .build()?;
    let bridge = Bridge::new(openai, model, instructions)?;
    let (listener, address) = bind_loopback(port).await?;
    eprintln!(
        "Kepos Codex bridge listening on http://{address}{path}",
        path = kepos_codex_bridge::ENDPOINT
    );
    bridge.serve(listener).await?;
    Ok(())
}

async fn login(auth_file: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = auth_file.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let login = ChatGptLogin::start(&auth_file).await?;
    println!(
        "Open this URL to authorize the bridge:\n\n{}",
        login.authorization_url()
    );
    let status = login.complete().await?;
    validate_private_auth_file(&auth_file)?;
    println!("Logged in to ChatGPT account {}", status.account_id);
    Ok(())
}

#[allow(dead_code)]
fn _address_is_loopback(address: SocketAddr) -> bool {
    address.ip().is_loopback()
}
