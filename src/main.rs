use anyhow::Result;
use remarkable_server::{
    create_router, AppState, DeviceManager, 
    ServerConfig, Storage
};
use std::env;
use std::path::PathBuf;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    // Install crypto provider for rustls
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install crypto provider");

    let config = parse_args();
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "remarkable_server=debug,tower_http=debug".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();
    
    std::fs::create_dir_all(&config.storage_path)?;
    
    // Initialize storage
    let storage = Storage::new(&config.storage_path)?;
    let stats = storage.stats();
    tracing::info!("Storage: {} files, {} bytes", stats.file_count, stats.total_bytes);
    
    // Initialize device manager
    let db_path = PathBuf::from(&config.storage_path).join("devices.db");
    let devices = DeviceManager::new(&db_path, &config.region, &config.bind)?;
    tracing::info!("Devices: {} registered", devices.list_devices()?.len());
    
    // Create app state
    let state = AppState::new(storage, devices);
    
    // Create router
    let app = create_router(state);
    
    // Start server - TLS or plain
    if let (Some(cert_path), Some(key_path)) = (&config.cert_path, &config.key_path) {
        // TLS mode
        use axum_server::tls_rustls::RustlsConfig;
        use std::net::SocketAddr;
        
        let tls_config = RustlsConfig::from_pem_file(cert_path, key_path).await?;
        let addr: SocketAddr = config.bind.parse()?;
        
        tracing::info!("Listening on {} (HTTPS)", config.bind);
        axum_server::bind_rustls(addr, tls_config)
            .serve(app.into_make_service())
            .await?;
    } else {
        // Plain HTTP mode
        let listener = tokio::net::TcpListener::bind(&config.bind).await?;
        tracing::info!("Listening on {} (HTTP)", config.bind);
        axum::serve(listener, app).await?;
    }
    
    Ok(())
}

fn parse_args() -> ServerConfig {
    let mut config = ServerConfig::default();
    
    let args: Vec<String> = env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--bind" | "-b" => {
                i += 1;
                if i < args.len() {
                    config.bind = args[i].clone();
                }
            }
            "--storage" | "-s" => {
                i += 1;
                if i < args.len() {
                    config.storage_path = args[i].clone();
                }
            }
            "--region" | "-r" => {
                i += 1;
                if i < args.len() {
                    config.region = args[i].clone();
                }
            }
            "--cert" | "-c" => {
                i += 1;
                if i < args.len() {
                    config.cert_path = Some(args[i].clone());
                }
            }
            "--key" | "-k" => {
                i += 1;
                if i < args.len() {
                    config.key_path = Some(args[i].clone());
                }
            }
            "--help" | "-h" => {
                println!("remarkable-server - Local reMarkable sync server");
                println!();
                println!("Usage: remarkable-server [OPTIONS]");
                println!();
                println!("Options:");
                println!("  -b, --bind <ADDR>     Address to bind (default: 127.0.0.1:8080)");
                println!("  -s, --storage <PATH>  Storage directory (default: ./remarkable-storage)");
                println!("  -r, --region <NAME>   Region name (default: local)");
                println!("  -c, --cert <PATH>     TLS certificate file (PEM)");
                println!("  -k, --key <PATH>      TLS private key file (PEM)");
                println!("  -h, --help            Show this help");
                std::process::exit(0);
            }
            _ => {}
        }
        i += 1;
    }
    
    config
}
