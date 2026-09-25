//! rm-proxy: runs on the reMarkable tablet.
//!
//! xochitl talks to `*.remarkable.com`. /etc/hosts points those names at
//! 127.0.0.1, where this relay terminates TLS with a locally trusted cert,
//! then opens a *publicly verified* TLS connection to the self-hosted server
//! (e.g. remarkable.unwrap.rs) and copies bytes in both directions. Because it
//! is a byte relay, HTTP, chunked uploads and WebSockets all pass unchanged.

use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{io::copy_bidirectional, net::{TcpListener, TcpStream}};
use tokio_rustls::{
    rustls::{self, pki_types::ServerName, ClientConfig, RootCertStore, ServerConfig},
    TlsAcceptor, TlsConnector,
};

struct Route { listen: SocketAddr, upstream: String }
struct Args { routes: Vec<Route>, cert: String, key: String, extra_ca: Option<String> }

fn usage() -> ! {
    eprintln!("usage: rm-proxy --cert FILE --key FILE [--upstream-ca FILE] \\
    (--upstream HOST[:PORT] [--listen 127.0.0.1:443] | --route LISTEN=HOST[:PORT] ...)

  --upstream/--listen  single relay (listen defaults to 127.0.0.1:443)
  --route              repeatable; e.g. --route 127.0.0.1:443=cloud.example.com:443
                                        --route 127.0.0.2:443=cloud.example.com:8883");
    std::process::exit(2)
}

fn with_port(mut hp: String) -> String { if !hp.contains(':') { hp.push_str(":443") } hp }

fn parse_args() -> Args {
    let mut a = Args { routes: Vec::new(), cert: String::new(), key: String::new(), extra_ca: None };
    let (mut listen, mut upstream): (SocketAddr, Option<String>) = ("127.0.0.1:443".parse().unwrap(), None);
    let mut it = std::env::args().skip(1);
    while let Some(f) = it.next() {
        let mut v = || it.next().unwrap_or_else(|| usage());
        match f.as_str() {
            "--listen" => listen = v().parse().unwrap_or_else(|_| usage()),
            "--cert" => a.cert = v(),
            "--key" => a.key = v(),
            "--upstream" => upstream = Some(v()),
            "--route" => {
                let r = v();
                let (l, u) = r.split_once('=').unwrap_or_else(|| usage());
                a.routes.push(Route { listen: l.parse().unwrap_or_else(|_| usage()), upstream: with_port(u.to_string()) });
            }
            "--upstream-ca" => a.extra_ca = Some(v()),
            "-h" | "--help" => usage(),
            _ => usage(),
        }
    }
    if let Some(u) = upstream { a.routes.push(Route { listen, upstream: with_port(u) }) }
    if a.cert.is_empty() || a.key.is_empty() || a.routes.is_empty() { usage() }
    a
}

fn load_certs(p: &str) -> Vec<rustls::pki_types::CertificateDer<'static>> {
    let mut r = std::io::BufReader::new(std::fs::File::open(p).unwrap_or_else(|e| panic!("open {p}: {e}")));
    rustls_pemfile::certs(&mut r).collect::<Result<_, _>>().unwrap_or_else(|e| panic!("parse {p}: {e}"))
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let a = parse_args();
    let provider = Arc::new(rustls::crypto::ring::default_provider());

    let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(
        std::fs::File::open(&a.key).unwrap_or_else(|e| panic!("open {}: {e}", a.key)),
    )).expect("parse key").expect("no private key in file");
    let server_cfg = ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions().unwrap()
        .with_no_client_auth()
        .with_single_cert(load_certs(&a.cert), key)
        .expect("bad cert/key");
    let acceptor = TlsAcceptor::from(Arc::new(server_cfg));

    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    if let Some(ca) = &a.extra_ca { for c in load_certs(ca) { roots.add(c).expect("bad --upstream-ca") } }
    let client_cfg = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions().unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(client_cfg));

    let mut tasks = Vec::new();
    for r in a.routes {
        let host = r.upstream.rsplit_once(':').unwrap().0.to_string();
        let sni = ServerName::try_from(host.clone()).expect("invalid upstream host");
        let listener = TcpListener::bind(r.listen).await.unwrap_or_else(|e| panic!("bind {}: {e}", r.listen));
        eprintln!("rm-proxy: {} -> {} (TLS verified as {host})", r.listen, r.upstream);
        tasks.push(tokio::spawn(relay(listener, acceptor.clone(), connector.clone(), sni, r.upstream)));
    }
    for t in tasks { let _ = t.await; }
}

async fn relay(listener: TcpListener, acceptor: TlsAcceptor, connector: TlsConnector, sni: ServerName<'static>, upstream: String) {
    loop {
        let (sock, peer) = match listener.accept().await { Ok(x) => x, Err(e) => { eprintln!("accept: {e}"); tokio::time::sleep(Duration::from_millis(200)).await; continue } };
        let (acceptor, connector, sni, upstream) = (acceptor.clone(), connector.clone(), sni.clone(), upstream.clone());
        tokio::spawn(async move {
            let _ = sock.set_nodelay(true);
            let mut down = match tokio::time::timeout(Duration::from_secs(15), acceptor.accept(sock)).await {
                Ok(Ok(s)) => s, Ok(Err(e)) => { eprintln!("{peer}: local TLS: {e}"); return } Err(_) => return,
            };
            let tcp = match tokio::time::timeout(Duration::from_secs(15), TcpStream::connect(&upstream)).await {
                Ok(Ok(t)) => t, Ok(Err(e)) => { eprintln!("{peer}: connect {upstream}: {e}"); return } Err(_) => { eprintln!("{peer}: connect {upstream}: timeout"); return }
            };
            let _ = tcp.set_nodelay(true);
            let mut up = match connector.connect(sni, tcp).await {
                Ok(s) => s, Err(e) => { eprintln!("{peer}: upstream TLS to {upstream}: {e}"); return }
            };
            // Ungraceful closes (no close_notify) are normal for xochitl; don't log them.
            let _ = copy_bidirectional(&mut down, &mut up).await;
        });
    }
}
