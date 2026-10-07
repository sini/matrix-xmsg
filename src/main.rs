use clap::Parser;
use matrix_xmsg::config::Config;
use matrix_xmsg::store::Store;
use std::path::PathBuf;
use tracing::{error, info};

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Matrix support bot backed by an expert agent session via xmsg"
)]
struct Args {
    #[arg(short, long, help = "Path to TOML configuration file")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    info!("Loading configuration from {}", args.config.display());

    let config = match Config::from_file(&args.config) {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to load configuration: {e}");
            std::process::exit(1);
        }
    };

    let token = match config.load_access_token() {
        Ok(t) => t,
        Err(e) => {
            error!(
                "Failed to read access token from {}: {e}",
                config.access_token_file.display()
            );
            std::process::exit(1);
        }
    };

    let _store = match Store::new(&config.db_path) {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to open store at {}: {e}", config.db_path.display());
            std::process::exit(1);
        }
    };

    info!(
        "matrix-xmsg daemon initialized for bot {} on homeserver {}",
        config.bot_mxid, config.homeserver_url
    );
    info!(
        "Allowlisted rooms: {:?}, trusted users: {}, token len: {}",
        config.rooms,
        config.trusted_mxids.len(),
        token.len()
    );

    // In M1, scaffold daemon runs until shutdown signal.
    // Live homeserver sync loop is wired in M2.
    tokio::signal::ctrl_c().await?;
    info!("Shutting down matrix-xmsg");

    Ok(())
}
