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

    let matrix_client = std::sync::Arc::new(
        match matrix_xmsg::matrix::MatrixSdkClient::new(
            &config.homeserver_url,
            &config.bot_mxid,
            &token,
        )
        .await
        {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to initialize Matrix client: {e}");
                std::process::exit(1);
            }
        },
    );

    let register_sock = config.xmsg_register_socket();
    let svc_inbox = match matrix_xmsg::xmsg::register_svc(&register_sock, "matrix-xmsg").await {
        Ok(inbox) => inbox,
        Err(e) => {
            error!(
                "Failed to register on xmsg register.sock at {}: {e}",
                register_sock.display()
            );
            std::process::exit(1);
        }
    };
    info!("Registered on xmsg as {}", svc_inbox.session_id());

    let xmsg_client = matrix_xmsg::xmsg::create_xmsg_client(&config);

    let tracker = matrix_xmsg::bot::register_event_handlers(
        matrix_client.inner(),
        config.clone(),
        matrix_client.clone(),
        xmsg_client,
        store.clone(),
    );

    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
    let shutdown_tx_ctrl_c = shutdown_tx.clone();

    let inbox_config = config.clone();
    let inbox_matrix = matrix_client.clone();
    let inbox_store = store.clone();
    let inbox_shutdown_rx = shutdown_tx.subscribe();
    tokio::spawn(async move {
        if let Err(e) = matrix_xmsg::bot::run_inbox_loop(
            svc_inbox,
            inbox_config,
            inbox_matrix,
            inbox_store,
            inbox_shutdown_rx,
        )
        .await
        {
            error!("Fatal inbox loop error: {e}");
        }
    });

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
