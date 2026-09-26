use std::env;
use std::path::PathBuf;

use anyhow::Result;
use remarkable_server::{AppState, DeviceManager, ServerConfig, Storage, create_router};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Single local account every paired device syncs into.
const PAIRING_USER: &str = "local-user";

#[tokio::main]
async fn main() -> Result<()> {
    // Install crypto provider for rustls
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install crypto provider");

    let config = parse_args();
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                "remarkable_server=debug,tower_http=debug,rustls=error,tokio_rustls=error".into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    std::fs::create_dir_all(&config.storage_path)?;

    // Initialize storage
    let storage = Storage::new(&config.storage_path)?;
    let stats = storage.stats();
    tracing::info!(
        "Storage: {} files, {} bytes",
        stats.file_count,
        stats.total_bytes
    );

    // Initialize device manager
    let db_path = PathBuf::from(&config.storage_path).join("devices.db");
    let devices = DeviceManager::new(&db_path, &config.region, &config.host)?;
    tracing::info!("Devices: {} registered", devices.list_devices(None)?.len());

    // `--pair`: print a one-time code to enter on the tablet (Settings > Account > Connect), then exit.
    if env::args().any(|a| a == "--pair") {
        let code = devices.create_pairing_code(PAIRING_USER)?;
        println!("{code}");
        return Ok(());
    }

    // Create app state (ICE servers are shared by the REST screenshare broker)
    let ice_servers: serde_json::Value =
        serde_json::from_str(&env::var("SCREENSHARE_ICE_SERVERS").unwrap_or_else(|_| "[]".into()))
            .unwrap_or_else(|_| serde_json::json!([]));
    let state = AppState::new(storage, devices)
        .with_ice_servers(ice_servers)
        .with_public_url(env::var("PUBLIC_URL").ok().as_deref());

    // Create router
    // Inbound email -> documents: an SMTP listener, only when EMAIL_INBOUND_BIND is set.
    let email_server = match env::var("EMAIL_INBOUND_BIND")
        .ok()
        .filter(|b| !b.is_empty())
    {
        Some(bind) => {
            let cfg = remarkable_server::email::EmailConfig {
                smtp_bind: bind,
                ..Default::default()
            };
            let server = remarkable_server::email::EmailServer::new(
                cfg,
                state.storage.clone(),
                state.devices.clone(),
                &PathBuf::from(&config.storage_path).join("emails.db"),
            )?;
            let runner = server.clone();
            tokio::spawn(async move {
                if let Err(e) = runner.run().await {
                    tracing::error!("inbound email server stopped: {e}");
                }
            });
            Some(server)
        }
        None => None,
    };

    // Screenshare signaling broker (MQTT over TLS). The tablet dials
    // vernemq-prod.cloud.remarkable.engineering:443, so bind it on its own address.
    let mut screenshare_broker = None;
    if let Some(bind) = env::var("SCREENSHARE_BIND").ok().filter(|b| !b.is_empty()) {
        // The broker is TLS-only. SCREENSHARE_CERT/SCREENSHARE_KEY let it use its own
        // cert when the HTTP listener runs plain behind a reverse proxy.
        let cert = env::var("SCREENSHARE_CERT")
            .ok()
            .filter(|v| !v.is_empty())
            .or_else(|| config.cert_path.clone());
        let key = env::var("SCREENSHARE_KEY")
            .ok()
            .filter(|v| !v.is_empty())
            .or_else(|| config.key_path.clone());
        let (Some(cert), Some(key)) = (cert, key) else {
            anyhow::bail!(
                "SCREENSHARE_BIND needs a cert: set SCREENSHARE_CERT/SCREENSHARE_KEY or pass --cert/--key"
            );
        };
        let tls = screenshare_tls(&cert, &key)?;
        let broker = remarkable_server::screenshare::Broker::new(
            state.devices.clone(),
            (*state.ice_servers).clone(),
        );
        screenshare_broker = Some(broker.clone());
        let addr: std::net::SocketAddr = bind.parse()?;
        tokio::spawn(async move {
            if let Err(e) = broker.serve(addr, tls).await {
                tracing::error!("screenshare broker stopped: {e}");
            }
        });
    }

    // Take revoked devices out of REST screenshare rooms.
    remarkable_server::screenshare_rest::spawn_revocation_cleanup(
        state.devices.clone(),
        state.screenshare.clone(),
    );

    // Keep the handwriting-search cache warm (recognises new pages after each sync).
    remarkable_server::hw_search::spawn_indexer(state.storage.clone());

    let features = remarkable_server::feature_routes(
        state.clone(),
        std::path::Path::new(&config.storage_path),
        email_server,
    )?;
    let viewer = screenshare_viewer(screenshare_broker, &state)?;
    let mut app = create_router(state).merge(features);
    if let Some(viewer) = viewer {
        app = app.merge(remarkable_server::screenshare_viewer::router(viewer));
    }

    // Start server - TLS or plain
    if let (Some(cert_path), Some(key_path)) = (&config.cert_path, &config.key_path) {
        // TLS mode
        use std::net::SocketAddr;

        use axum_server::tls_rustls::RustlsConfig;

        let tls_config = RustlsConfig::from_pem_file(cert_path, key_path).await?;
        let addr: SocketAddr = config.bind.parse()?;
        let listener = remarkable_server::bind_when_available(addr)
            .await?
            .into_std()?;

        tracing::info!("Listening on {} (HTTPS)", config.bind);
        axum_server::from_tcp_rustls(listener, tls_config)?
            .serve(app.into_make_service_with_connect_info::<SocketAddr>())
            .await?;
    } else {
        // Plain HTTP mode
        let listener = remarkable_server::bind_when_available(config.bind.parse()?).await?;
        tracing::info!("Listening on {} (HTTP)", config.bind);
        // Peer address feeds the OAuth device-code rate limiter (see `oauth::ClientIp`).
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await?;
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
            "--host" | "-H" => {
                i += 1;
                if i < args.len() {
                    config.host = args[i].clone();
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
                println!(
                    "  -H, --host <NAME>     Hostname the device uses to reach us (default: local.tectonic.remarkable.com)"
                );
                println!(
                    "  -s, --storage <PATH>  Storage directory (default: ./remarkable-storage)"
                );
                println!("  -r, --region <NAME>   Region name (default: local)");
                println!("  -c, --cert <PATH>     TLS certificate file (PEM)");
                println!("  -k, --key <PATH>      TLS private key file (PEM)");
                println!(
                    "      --pair            Print a one-time pairing code for a new device and exit"
                );
                println!("  -h, --help            Show this help");
                std::process::exit(0);
            }
            _ => {}
        }
        i += 1;
    }

    config
}

/// Browser viewer for the tablet's screen share (`/screenshare/view`), when
/// `SCREENSHARE_VIEWER=1`. Needs `ADMIN_TOKEN`, which guards it. Watches tablets
/// on the MQTT broker (when `SCREENSHARE_BIND` is set) and on the REST rooms.
fn screenshare_viewer(
    broker: Option<remarkable_server::screenshare::Broker>,
    state: &AppState,
) -> Result<Option<remarkable_server::screenshare_viewer::ScreenViewer>> {
    use remarkable_server::screenshare_viewer::{RestRooms, ScreenViewer, Signaling, ViewerConfig};
    if !matches!(
        env::var("SCREENSHARE_VIEWER").as_deref(),
        Ok("1" | "true" | "on")
    ) {
        return Ok(None);
    }
    if env::var("ADMIN_TOKEN").map_or(true, |t| t.trim().is_empty()) {
        anyhow::bail!("SCREENSHARE_VIEWER needs ADMIN_TOKEN, which protects the viewer page");
    }
    let udp_ports = match env::var("SCREENSHARE_VIEWER_UDP_PORTS")
        .ok()
        .filter(|v| !v.is_empty())
    {
        Some(range) => {
            let (min, max) = range.split_once('-').ok_or_else(|| {
                anyhow::anyhow!("SCREENSHARE_VIEWER_UDP_PORTS must look like 50000-50100")
            })?;
            Some((min.trim().parse()?, max.trim().parse()?))
        }
        None => None,
    };
    let ice_servers = env::var("SCREENSHARE_VIEWER_ICE")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(remarkable_screenshare::IceServer::url)
        .collect();
    let user_id = env::var("SCREENSHARE_VIEWER_USER")
        .ok()
        .filter(|u| !u.is_empty())
        .unwrap_or_else(|| PAIRING_USER.into());
    tracing::info!(user = %user_id, ?udp_ports, "screenshare browser viewer enabled at /screenshare/view");
    let signaling = Signaling {
        mqtt: broker,
        rest: Some(RestRooms {
            rooms: state.screenshare.clone(),
            notifications: state.notification_tx.clone(),
        }),
    };
    Ok(Some(ScreenViewer::new(
        signaling,
        ViewerConfig {
            user_id,
            transport: remarkable_screenshare::TransportConfig {
                ice_servers,
                udp_ports,
            },
            idle_grace: remarkable_server::screenshare_viewer::IDLE_GRACE,
            reports_dir: Some(state.storage.base_path().to_path_buf()),
        },
    )))
}

/// TLS acceptor for the screenshare broker, from the same PEM cert/key as HTTPS.
fn screenshare_tls(cert: &str, key: &str) -> Result<tokio_rustls::TlsAcceptor> {
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    let certs = CertificateDer::pem_file_iter(cert)?.collect::<std::result::Result<Vec<_>, _>>()?;
    let key = PrivateKeyDer::from_pem_file(key)?;
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    Ok(tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(config)))
}
