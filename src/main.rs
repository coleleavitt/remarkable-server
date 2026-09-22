//! reMarkable Sync Server CLI
//!
//! Usage:
//!   remarkable-server [OPTIONS]
//!
//! Options:
//!   -b, --bind <ADDR>      Bind address [default: 127.0.0.1:8080]
//!   -s, --storage <PATH>   Storage directory [default: ./remarkable-storage]
//!   -v, --verbose          Enable verbose logging

use anyhow::Result;
use remarkable_server::{create_router, AppState, ServerConfig, Storage};
use std::env;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    // Parse arguments
    let config = parse_args();
    
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "remarkable_server=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
    
    // Create storage
    let storage = Storage::new(&config.storage_path)?;
    let stats = storage.stats();
    
    tracing::info!(
        "Storage initialized: {} files, {} bytes, root={}, gen={}",
        stats.file_count,
        stats.total_bytes,
        if stats.root_hash.is_empty() { "<empty>" } else { &stats.root_hash },
        stats.generation
    );
    
    // Create app state and router
    let state = AppState::new(storage);
    let router = create_router(state);
    
    // Start server
    let listener = tokio::net::TcpListener::bind(&config.bind).await?;
    tracing::info!("Listening on http://{}", config.bind);
    tracing::info!("Endpoints:");
    tracing::info!("  GET  /sync/v3/root           - Get root hash");
    tracing::info!("  GET  /sync/v3/files/{{hash}}   - Download file");
    tracing::info!("  PUT  /sync/v3/files/{{hash}}   - Upload file");
    tracing::info!("  POST /token/json/2/user/new  - Refresh token (mock)");
    tracing::info!("  GET  /health                 - Health check");
    tracing::info!("  GET  /debug/files            - List files");
    tracing::info!("  DELETE /debug/clear          - Clear storage");
    
    axum::serve(listener, router).await?;
    
    Ok(())
}

fn parse_args() -> ServerConfig {
    let mut config = ServerConfig::default();
    let args: Vec<String> = env::args().collect();
    let mut i = 1;
    
    while i < args.len() {
        match args[i].as_str() {
            "-b" | "--bind" => {
                if i + 1 < args.len() {
                    config.bind = args[i + 1].clone();
                    i += 1;
                }
            }
            "-s" | "--storage" => {
                if i + 1 < args.len() {
                    config.storage_path = args[i + 1].clone();
                    i += 1;
                }
            }
            "-h" | "--help" => {
                println!("remarkable-server - Local reMarkable Sync Server");
                println!();
                println!("USAGE:");
                println!("    remarkable-server [OPTIONS]");
                println!();
                println!("OPTIONS:");
                println!("    -b, --bind <ADDR>     Bind address [default: 127.0.0.1:8080]");
                println!("    -s, --storage <PATH>  Storage directory [default: ./remarkable-storage]");
                println!("    -h, --help            Print help");
                std::process::exit(0);
            }
            _ => {}
        }
        i += 1;
    }
    
    config
}
