//! Name resolution for CalDAV requests that keeps other hosts off internal addresses.
//!
//! A CalDAV server's redirects and hrefs may lead to other hosts (iCloud serves calendar homes
//! from per-user hosts). Checking a host name before the request is not enough: the name may
//! resolve to a loopback or private address, or resolve to a public one when checked and to
//! an internal one when connected (DNS rebinding). So the CalDAV client resolves names
//! through [`GuardedResolver`], which drops internal addresses from the answer for every
//! host but the configured one, and the connection only ever uses the addresses it vetted.
//! The configured host itself may be internal (a Nextcloud on the LAN).
//!
//! IP literals are not looked up; [`super::caldav`] refuses internal ones for other hosts
//! before sending anything.

use std::future::Future;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;

use reqwest::dns::{Addrs, Name, Resolve, Resolving};

/// Looks up the addresses of a host name.
pub(crate) trait Lookup: Send + Sync {
    fn lookup(&self, host: String)
    -> Pin<Box<dyn Future<Output = io::Result<Vec<IpAddr>>> + Send>>;
}

/// The system resolver (`getaddrinfo` on a blocking thread), as reqwest uses by default.
pub(crate) struct SystemLookup;

impl Lookup for SystemLookup {
    fn lookup(
        &self,
        host: String,
    ) -> Pin<Box<dyn Future<Output = io::Result<Vec<IpAddr>>> + Send>> {
        Box::pin(async move {
            Ok(tokio::net::lookup_host((host.as_str(), 0))
                .await?
                .map(|addr| addr.ip())
                .collect())
        })
    }
}

/// Whether `ip` is not a public unicast address: loopback, private, link-local, CGNAT,
/// unspecified, multicast and reserved ranges, and IPv6 forms that embed such an IPv4
/// address.
pub(super) fn is_internal_ip(ip: IpAddr) -> bool {
    fn v4(ip: Ipv4Addr) -> bool {
        let [a, b, ..] = ip.octets();
        ip.is_loopback()
            || ip.is_private()
            || ip.is_link_local()
            || ip.is_unspecified()
            || ip.is_broadcast()
            || ip.is_multicast()
            || a == 0
            // 100.64.0.0/10, carrier-grade NAT.
            || (a == 100 && b & 0xc0 == 64)
            // 192.0.0.0/24, IETF protocol assignments.
            || (a == 192 && b == 0 && ip.octets()[2] == 0)
            // 198.18.0.0/15, benchmarking.
            || (a == 198 && b & 0xfe == 18)
            // 240.0.0.0/4, reserved.
            || a >= 240
    }
    fn v6(ip: Ipv6Addr) -> bool {
        let first = ip.segments()[0];
        ip.is_loopback()
            || ip.is_unspecified()
            || ip.is_multicast()
            || ip.is_unique_local()
            || ip.is_unicast_link_local()
            // fec0::/10, deprecated site-local.
            || first & 0xffc0 == 0xfec0
            || ip.to_ipv4_mapped().is_some_and(v4)
            // 64:ff9b::/96, NAT64 of an IPv4 address.
            || (ip.segments()[..6] == [0x64, 0xff9b, 0, 0, 0, 0]
                && v4(Ipv4Addr::from_bits(ip.to_bits() as u32)))
    }
    match ip {
        IpAddr::V4(ip) => v4(ip),
        IpAddr::V6(ip) => v6(ip),
    }
}

/// `host` compared case-insensitively and without a trailing dot.
fn normalize(host: &str) -> String {
    host.trim_end_matches('.').to_ascii_lowercase()
}

/// Hosts of the proxies reqwest takes from the environment (`ALL_PROXY`, `HTTP_PROXY`,
/// `HTTPS_PROXY`, either case). With a proxy, reqwest only resolves the proxy's name, and the
/// proxy reaches the servers.
pub(super) fn proxy_hosts(var: impl Fn(&str) -> Option<String>) -> Vec<String> {
    [
        "ALL_PROXY",
        "all_proxy",
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
    ]
    .into_iter()
    .filter_map(|name| var(name))
    .filter_map(|value| {
        let value = value.trim();
        let url = reqwest::Url::parse(value)
            .ok()
            .filter(|u| u.has_host())
            .or_else(|| reqwest::Url::parse(&format!("http://{}", value)).ok())?;
        url.host_str().map(normalize)
    })
    .collect()
}

/// Resolves through `lookup`, keeping only public addresses for hosts not in `trusted`.
pub(super) struct GuardedResolver {
    trusted: Vec<String>,
    lookup: Arc<dyn Lookup>,
}

impl GuardedResolver {
    /// `trusted`: the configured host (and the proxies), whose addresses are not filtered.
    pub(super) fn new<'a>(
        trusted: impl IntoIterator<Item = &'a str>,
        lookup: Arc<dyn Lookup>,
    ) -> Self {
        Self {
            trusted: trusted.into_iter().map(normalize).collect(),
            lookup,
        }
    }
}

impl Resolve for GuardedResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let host = normalize(name.as_str());
        let trusted = self.trusted.contains(&host);
        let lookup = self.lookup.lookup(host.clone());
        Box::pin(async move {
            let found = lookup.await?;
            let allowed: Vec<IpAddr> = if trusted {
                found.clone()
            } else {
                found
                    .iter()
                    .copied()
                    .filter(|ip| !is_internal_ip(*ip))
                    .collect()
            };
            if allowed.is_empty() {
                let reason = if found.is_empty() {
                    format!("{} has no addresses", host)
                } else {
                    format!(
                        "refusing to connect to {}: it resolves only to internal addresses",
                        host
                    )
                };
                return Err(io::Error::other(reason).into());
            }
            // Port 0: reqwest puts in the URL's port.
            let addrs: Addrs = Box::new(allowed.into_iter().map(|ip| SocketAddr::new(ip, 0)));
            Ok(addrs)
        })
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::collections::HashMap;
    use std::str::FromStr;

    use super::*;

    /// Fixed answers instead of the system resolver.
    pub(crate) struct FakeLookup(pub HashMap<String, Vec<IpAddr>>);

    impl FakeLookup {
        pub(crate) fn new(entries: &[(&str, &[&str])]) -> Arc<Self> {
            Arc::new(Self(
                entries
                    .iter()
                    .map(|(host, ips)| {
                        (
                            host.to_string(),
                            ips.iter().map(|ip| ip.parse().unwrap()).collect(),
                        )
                    })
                    .collect(),
            ))
        }
    }

    impl Lookup for FakeLookup {
        fn lookup(
            &self,
            host: String,
        ) -> Pin<Box<dyn Future<Output = io::Result<Vec<IpAddr>>> + Send>> {
            let found = self.0.get(&host).cloned();
            Box::pin(async move {
                found.ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no such host"))
            })
        }
    }

    async fn resolve(resolver: &GuardedResolver, host: &str) -> Result<Vec<String>, String> {
        match resolver.resolve(Name::from_str(host).unwrap()).await {
            Ok(addrs) => Ok(addrs.map(|a| a.ip().to_string()).collect()),
            Err(e) => Err(e.to_string()),
        }
    }

    #[tokio::test]
    async fn other_hosts_get_only_public_addresses() {
        let lookup = FakeLookup::new(&[
            ("nextcloud.lan", &["192.168.1.5"]),
            ("rebind.example", &["127.0.0.1"]),
            ("metadata.example", &["169.254.169.254", "fd00::1"]),
            (
                "mixed.example",
                &["10.0.0.7", "203.0.113.9", "::1", "2001:db8::5"],
            ),
            ("public.example", &["203.0.113.10"]),
        ]);
        let resolver = GuardedResolver::new(["Nextcloud.LAN."], lookup);
        // The configured host may be internal.
        assert_eq!(
            resolve(&resolver, "nextcloud.lan").await.unwrap(),
            ["192.168.1.5"]
        );
        for host in ["rebind.example", "metadata.example"] {
            let err = resolve(&resolver, host).await.unwrap_err();
            assert!(
                err.contains("resolves only to internal addresses"),
                "{}: {}",
                host,
                err
            );
        }
        // Only the vetted addresses are handed to the connector.
        assert_eq!(
            resolve(&resolver, "mixed.example").await.unwrap(),
            ["203.0.113.9", "2001:db8::5"]
        );
        assert_eq!(
            resolve(&resolver, "PUBLIC.example.").await.unwrap(),
            ["203.0.113.10"]
        );
        assert!(resolve(&resolver, "missing.example").await.is_err());
    }

    #[test]
    fn internal_ranges() {
        for ip in [
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "192.168.0.1",
            "169.254.169.254",
            "100.64.0.1",
            "0.0.0.0",
            "0.1.2.3",
            "255.255.255.255",
            "224.0.0.1",
            "192.0.0.8",
            "198.18.0.1",
            "240.0.0.1",
            "::",
            "::1",
            "fd00::1",
            "fe80::1",
            "fec0::1",
            "ff02::1",
            "::ffff:127.0.0.1",
            "64:ff9b::a00:1",
        ] {
            assert!(is_internal_ip(ip.parse().unwrap()), "{}", ip);
        }
        for ip in [
            "203.0.113.9",
            "8.8.8.8",
            "100.128.0.1",
            "2001:db8::1",
            "64:ff9b::808:808",
        ] {
            assert!(!is_internal_ip(ip.parse().unwrap()), "{}", ip);
        }
    }

    #[test]
    fn proxy_hosts_come_from_the_environment_variables() {
        let env: HashMap<&str, &str> = HashMap::from([
            ("HTTPS_PROXY", "http://user:pw@Proxy.corp:3128"),
            ("http_proxy", "squid.lan:8080"),
            ("NO_PROXY", "ignored.example"),
        ]);
        let hosts = proxy_hosts(|name| env.get(name).map(|v| v.to_string()));
        assert_eq!(hosts, ["squid.lan", "proxy.corp"]);
        assert!(proxy_hosts(|_| None).is_empty());
    }
}
