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

    #[arg(
        long,
        help = "Run E2EE setup/bootstrap oneshot and exit after initializing crypto store"
    )]
    e2ee_setup: bool,

    #[arg(
        long,
        help = "Permit store-loss recovery from secret storage when the local crypto store is empty and cross-signing exists on the server (never deletes a non-empty store)"
    )]
    reset_crypto_store: bool,

    #[arg(
        long,
        help = "Path to the secret-storage recovery key file (oneshot setup/reset only)"
    )]
    recovery_key_file: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

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

    let passphrase = match config.load_store_passphrase() {
        Ok(p) => p,
        Err(e) => {
            error!("Failed to load crypto store passphrase: {e}");
            std::process::exit(1);
        }
    };

    let recovery_key = match args.recovery_key_file.as_ref() {
        Some(path) => match Config::load_recovery_key(path) {
            Ok(k) => Some(k),
            Err(e) => {
                error!("Failed to read recovery key from {}: {e}", path.display());
                std::process::exit(1);
            }
        },
        None => None,
    };

    let crypto_dir = config.crypto_store_dir();

    let config = std::sync::Arc::new(config);
    let store = std::sync::Arc::new(match Store::new(&config.db_path) {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to open store at {}: {e}", config.db_path.display());
            std::process::exit(1);
        }
    });

    info!(
        "matrix-xmsg daemon initializing for bot {} on homeserver {}",
        config.bot_mxid, config.homeserver_url
    );
    info!(
        "Allowlisted rooms: {:?}, trusted users: {}",
        config.rooms,
        config.trusted_mxids.len()
    );

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel(1);
    let shutdown_tx_ctrl_c = shutdown_tx.clone();
    tokio::spawn(async move {
        if let Ok(()) = tokio::signal::ctrl_c().await {
            info!("Received SIGINT/ctrl-c, initiating graceful shutdown");
            let _ = shutdown_tx_ctrl_c.send(());
        }
    });

    #[cfg(unix)]
    {
        let shutdown_tx_sigterm = shutdown_tx.clone();
        tokio::spawn(async move {
            if let Ok(mut sig) =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            {
                sig.recv().await;
                info!("Received SIGTERM, initiating graceful shutdown");
                let _ = shutdown_tx_sigterm.send(());
            }
        });
    }

    let matrix_client = loop {
        match matrix_xmsg::matrix::MatrixSdkClient::new_with_e2ee(
            &config.homeserver_url,
            &config.bot_mxid,
            &token,
            &crypto_dir,
            &passphrase,
            args.reset_crypto_store,
            recovery_key.as_deref(),
        )
        .await
        {
            Ok(c) => break std::sync::Arc::new(c),
            Err(e) => {
                let err_str = e.to_string();
                if !args.e2ee_setup && err_str.contains("status_code: 401") {
                    tracing::warn!("Sync error: {e}, retrying in 3 seconds");
                    tokio::select! {
                        _ = shutdown_rx.recv() => {
                            info!("Shutdown signal received during startup retry, exiting cleanly");
                            return Ok(());
                        }
                        _ = tokio::time::sleep(std::time::Duration::from_secs(3)) => continue,
                    }
                }
                error!("Failed to initialize Matrix client: {e}");
                std::process::exit(1);
            }
        }
    };

    if args.e2ee_setup {
        info!("matrix-xmsg E2EE setup complete");
        return Ok(());
    }

    // Resolve configured rooms in client state before serving the inbox
    if let Err(e) = matrix_client.resolve_rooms(&config.rooms).await {
        tracing::warn!("Failed resolving configured rooms in client state: {e}");
    }

    let register_sock = config.xmsg_register_socket();
    let svc_inbox = matrix_xmsg::xmsg::ReconnectingSvcInbox::new(register_sock, "matrix-xmsg");

    let xmsg_client = matrix_xmsg::xmsg::create_xmsg_client(&config);

    let tracker = matrix_xmsg::bot::register_event_handlers(
        matrix_client.inner(),
        config.clone(),
        matrix_client.clone(),
        xmsg_client.clone(),
        store.clone(),
    );

    let inbox_config = config.clone();
    let inbox_matrix = matrix_client.clone();
    let inbox_store = store.clone();
    let inbox_xmsg = xmsg_client.clone();
    let inbox_shutdown_rx = shutdown_tx.subscribe();
    tokio::spawn(async move {
        if let Err(e) = matrix_xmsg::bot::run_inbox_loop(
            svc_inbox,
            inbox_config,
            inbox_matrix,
            inbox_store,
            inbox_xmsg,
            inbox_shutdown_rx,
        )
        .await
        {
            error!("Fatal inbox loop error: {e}");
        }
    });

    info!("Starting Matrix event sync loop");
    if let Err(e) = matrix_xmsg::bot::run_daemon_loop_with_drain(
        matrix_client.inner(),
        store,
        matrix_client.as_ref(),
        &tracker,
        std::time::Duration::from_secs(5),
        shutdown_rx,
    )
    .await
    {
        error!("Fatal sync loop error: {e}");
        std::process::exit(1);
    }

    info!("matrix-xmsg stopped cleanly");
    Ok(())
}
