use anyhow::Result;
use remarkable_server::{create_router, AppState, DeviceManager, ServerConfig, Storage};
use std::env;
use std::path::PathBuf;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    let config = parse_args();
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "remarkable_server=debug,tower_http=debug".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();
    std::fs::create_dir_all(&config.storage_path)?;
    let storage = Storage::new(&config.storage_path)?;
    let stats = storage.stats();
    tracing::info!("Storage: {} files, {} bytes", stats.file_count, stats.total_bytes);
    let db_path = PathBuf::from(&config.storage_path).join("devices.db");
    let devices = DeviceManager::new(&db_path, &config.region, &config.bind)?;
    tracing::info!("Devices: {} registered", devices.list_devices()?.len());
    let state = AppState::new(storage, devices);
    let router = create_router(state);
    let listener = tokio::net::TcpListener::bind(&config.bind).await?;
    tracing::info!("Listening on http://{}", config.bind);
    axum::serve(listener, router).await?;
    Ok(())
}

fn parse_args() -> ServerConfig {
    let mut config = ServerConfig::default();
    let args: Vec<String> = env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-b" | "--bind" if i + 1 < args.len() => { config.bind = args[i + 1].clone(); i += 1; }
            "-s" | "--storage" if i + 1 < args.len() => { config.storage_path = args[i + 1].clone(); i += 1; }
            "-r" | "--region" if i + 1 < args.len() => { config.region = args[i + 1].clone(); i += 1; }
            "-h" | "--help" => { println!("remarkable-server [OPTIONS]\n  -b, --bind <ADDR>\n  -s, --storage <PATH>\n  -r, --region <REGION>"); std::process::exit(0); }
            _ => {}
        }
        i += 1;
    }
    config
}
