//! Entry point of the iog application (I/O Gateway).
//!
//! Usage: `iog <path_to_config>`
//!
//! The application runs in one of two modes, determined by the `mode` field
//! in the `[general]` section of the configuration file: `client` or `server`.
//!
//! While running, the following commands are accepted from standard input:
//! - *(empty line)* — print connection and traffic statistics;
//! - `p` — send a Ping packet manually (client mode only).

use std::process::exit;
use std::sync::Arc;

use clap::Parser;
use log::{error, info};
use tokio::io::{AsyncBufReadExt, BufReader};

mod configuration;
mod consts;
mod gateway;

use configuration::Configuration;
use consts::AppMode;
use gateway::client::GatewayClient;
use gateway::server::GatewayServer;
use gateway::metrics;
use gateway::GatewayTrait;

/// Application command-line arguments.
#[derive(Parser, Debug)]
#[command(
    name = "iog",
    version,
    about = "I/O Gateway — asynchronous network gateway with a binary protocol",
    long_about = None
)]
struct Args {
    /// Path to the configuration file (INI, TOML or JSON).
    config: String,
}

/// Ignores the `SIGPIPE` signal so that writing to a closed socket
/// does not terminate the process.
fn ignore_sigpipe() {
    #[cfg(unix)]
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
    }
}

/// Handles standard input commands in client mode.
async fn stdin_loop_client(client: Arc<GatewayClient>) {
    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => match line.trim() {
                "" => client.print_stats().await,
                "p" | "P" => client.send_ping().await,
                other => info!("Unknown command: '{}' (available: Enter, p)", other),
            },
            Ok(None) => break,
            Err(e) => {
                error!("stdin read error: {}", e);
                break;
            }
        }
    }
}

/// Handles standard input commands in server mode.
async fn stdin_loop_server(server: Arc<GatewayServer>) {
    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => match line.trim() {
                "" => server.print_stats().await,
                other => info!("Unknown command: '{}' (available: Enter)", other),
            },
            Ok(None) => break,
            Err(e) => {
                error!("stdin read error: {}", e);
                break;
            }
        }
    }
}

/// Waits for a signal to perform a graceful shutdown.
///
/// Implements SIGTERM and Ctrl+C handling for a correct shutdown.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("Shutdown signal received, starting graceful shutdown");
}

/// Main function of the application.
#[tokio::main]
async fn main() {
    env_logger::init();
    ignore_sigpipe();

    let args = Args::parse();

    // Load and validate the configuration.
    let config = match Configuration::load(&args.config) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Configuration load error '{}': {}", args.config, e);
            exit(1);
        }
    };

    if let Err(e) = config.validate() {
        eprintln!("Configuration validation error: {}", e);
        exit(1);
    }

    let mode = config.app_mode();
    info!("iog started in mode: {}", mode);
    println!("iog is running in mode: {}", mode);

    // Initialize and start the Prometheus metrics server.
    let metrics_port = config.metrics_port().unwrap_or(metrics::DEFAULT_METRICS_PORT);
    tokio::spawn(async move {
        metrics::start_metrics_server(metrics_port).await;
    });

    // Start the gateway in the selected mode with graceful shutdown support.
    match mode {
        AppMode::Client => {
            let client = Arc::new(GatewayClient::new(config));
            let client_stdin = client.clone();
            let client_handle = client.clone();
            
            tokio::spawn(async move {
                stdin_loop_client(client_stdin).await;
            });

            let shutdown = shutdown_signal();
            let client_task = tokio::spawn(async move {
                client_handle.run().await;
            });
            tokio::pin!(client_task);

            tokio::select! {
                _ = shutdown => {
                    info!("Initiating graceful shutdown for client");
                    GatewayTrait::shutdown(&*client).await;
                    // Finish the task after graceful shutdown
                    client_task.as_mut().abort();
                }
                result = &mut client_task => {
                    result.unwrap();
                }
            }
        }
        AppMode::Server => {
            let server = Arc::new(GatewayServer::new(config));
            let server_stdin = server.clone();
            let server_handle = server.clone();
            
            tokio::spawn(async move {
                stdin_loop_server(server_stdin).await;
            });

            let shutdown = shutdown_signal();
            let server_task = tokio::spawn(async move {
                server_handle.run().await;
            });
            tokio::pin!(server_task);

            tokio::select! {
                _ = shutdown => {
                    info!("Initiating graceful shutdown for server");
                    GatewayTrait::shutdown(&*server).await;
                    // Finish the task after graceful shutdown
                    server_task.as_mut().abort();
                }
                result = &mut server_task => {
                    result.unwrap();
                }
            }
        }
        AppMode::Undefined => {
            eprintln!("Undefined operating mode (mode must be client or server).");
            exit(1);
        }
    }
}
