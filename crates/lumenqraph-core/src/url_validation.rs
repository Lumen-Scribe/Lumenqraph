//! URL validation to prevent SSRF attacks in webhook subscriptions.
//! Validates both at registration (quick checks) and at delivery time (DNS resolution).

use std::net::IpAddr;
use url::Url;

pub fn validate_webhook_url(url: &str) -> Result<(), String> {
    let parsed = Url::parse(url).map_err(|e| format!("invalid URL: {}", e))?;

    match parsed.scheme() {
        "http" | "https" => {}
        _ => return Err("url scheme must be http or https".to_string()),
    }

    // url 2.5.8 (WhatWG) returns a non-empty host_str() for URLs with an empty
    // authority (e.g. "http:///hook" → host_str() == Some("hook")).  Guard by
    // checking the raw authority section directly: if nothing appears between
    // "://" and the first path/query/fragment delimiter the URL has no host.
    let after_scheme = url.get(parsed.scheme().len() + 3..).unwrap_or("");
    let raw_host_end = after_scheme
        .find(['/', '?', '#', ':'])
        .unwrap_or(after_scheme.len());
    if after_scheme[..raw_host_end].is_empty() {
        return Err("url must have a host".to_string());
    }

    if let Some(host) = parsed.host_str() {
        if host.is_empty() {
            return Err("url must have a host".to_string());
        }

        if is_internal_address(host) {
            return Err(
                "url points to an internal/reserved address (loopback, link-local, private, or multicast)"
                    .to_string(),
            );
        }
    } else {
        return Err("url must have a host".to_string());
    }

    Ok(())
}

fn is_internal_address(host: &str) -> bool {
    if is_localhost(host) {
        return true;
    }

    // url 2.5.8 returns IPv6 addresses with surrounding brackets (e.g. "[ff00::1]").
    // Strip them so the string parses as a valid IpAddr.
    let addr_str = host
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(host);
    if let Ok(ip) = addr_str.parse::<IpAddr>() {
        return is_reserved_ip(&ip);
    }

    false
}

fn is_reserved_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_multicast()
                || v4.is_broadcast()
                || (v4.octets()[0] == 0)
                || is_documentation_v4(v4)
                || is_reserved_v4(v4)
                || is_shared_address_space_v4(v4)
                || is_benchmarking_v4(v4)
        }
        IpAddr::V6(v6) => {
            // Check for IPv4-mapped IPv6 addresses (::ffff:0:0/96)
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_reserved_ip(&IpAddr::V4(v4));
            }
            
            // Check for NAT64 well-known prefix (64:ff9b::/96)
            if is_nat64_wellknown(v6) {
                return true;
            }
            
            // Check for 6to4 (2002::/16) - may contain embedded IPv4
            if is_6to4(v6) {
                return true;
            }
            
            // Check for IPv4-compatible IPv6 (::/96 except ::/128 and ::1/128)
            if is_ipv4_compatible(v6) {
                return true;
            }
            
            v6.is_loopback()
                || v6.is_multicast()
                || v6.is_unicast_link_local()
                || v6.is_unspecified()
                || v6.is_unique_local()
                || is_documentation_v6(v6)
                || is_discard_prefix(v6)
        }
    }
}

fn is_documentation_v4(ip: &std::net::Ipv4Addr) -> bool {
    let octets = ip.octets();
    (octets[0] == 192 && octets[1] == 0 && octets[2] == 2)
        || (octets[0] == 198 && octets[1] == 51 && octets[2] == 100)
        || (octets[0] == 203 && octets[1] == 0 && octets[2] == 113)
}

fn is_reserved_v4(ip: &std::net::Ipv4Addr) -> bool {
    let octets = ip.octets();
    (octets[0] == 100 && octets[1] >= 64 && octets[1] <= 127)
        || (octets[0] == 192 && octets[1] == 168)
        || (octets[0] == 172 && octets[1] >= 16 && octets[1] <= 31)
        || (octets[0] == 10)
        || (octets[0] == 127)
        || (octets[0] == 169 && octets[1] == 254)
        || (octets[0] >= 240) // 240.0.0.0/4 - Reserved for future use
}

fn is_shared_address_space_v4(ip: &std::net::Ipv4Addr) -> bool {
    let octets = ip.octets();
    octets[0] == 100 && octets[1] >= 64 && octets[1] <= 127
}

fn is_benchmarking_v4(ip: &std::net::Ipv4Addr) -> bool {
    let octets = ip.octets();
    octets[0] == 198 && (octets[1] == 18 || octets[1] == 19)
}

fn is_nat64_wellknown(ip: &std::net::Ipv6Addr) -> bool {
    let segments = ip.segments();
    segments[0] == 0x64 && segments[1] == 0xff9b && segments[2] == 0 && segments[3] == 0
}

fn is_6to4(ip: &std::net::Ipv6Addr) -> bool {
    let segments = ip.segments();
    segments[0] == 0x2002
}

fn is_ipv4_compatible(ip: &std::net::Ipv6Addr) -> bool {
    let segments = ip.segments();
    segments[0] == 0
        && segments[1] == 0
        && segments[2] == 0
        && segments[3] == 0
        && segments[4] == 0
        && segments[5] == 0
        && !(segments[6] == 0 && (segments[7] == 0 || segments[7] == 1))
}

fn is_discard_prefix(ip: &std::net::Ipv6Addr) -> bool {
    let segments = ip.segments();
    segments[0] == 0x100
}

fn is_documentation_v6(ip: &std::net::Ipv6Addr) -> bool {
    let segments = ip.segments();
    segments[0] == 0x2001 && segments[1] == 0xdb8
}

fn is_localhost(host: &str) -> bool {
    matches!(
        host.to_lowercase().as_str(),
        "localhost" | "127.0.0.1" | "::1" | "[::1]"
    )
}

/// Validate webhook URL at delivery time by resolving hostname and checking resolved IPs.
/// This prevents DNS rebinding attacks where a URL initially validates but resolves
/// to an internal address at delivery time.
///
/// Returns the validated public IP address to be used for connection pinning.
pub async fn validate_webhook_url_at_delivery(url: &str) -> Result<IpAddr, String> {
    let parsed = Url::parse(url).map_err(|e| format!("invalid URL: {}", e))?;

    if let Some(host_str) = parsed.host_str() {
        // If it's already an IP address, validate it again (rules may have changed)
        let addr_str = host_str
            .strip_prefix('[')
            .and_then(|s| s.strip_suffix(']'))
            .unwrap_or(host_str);
        
        if let Ok(ip) = addr_str.parse::<IpAddr>() {
            // Re-validate literal IP URLs at delivery time
            if is_reserved_ip(&ip) {
                return Err("IP address points to an internal/reserved network".to_string());
            }
            return Ok(ip);
        }

        // Resolve hostname to IP addresses
        match tokio::net::lookup_host(format!("{}:80", host_str)).await {
            Ok(addrs) => {
                let addrs: Vec<_> = addrs.collect();
                
                if addrs.is_empty() {
                    return Err("hostname resolved to no addresses".to_string());
                }
                
                // Require ALL resolved addresses to be public
                for addr in &addrs {
                    if is_reserved_ip(&addr.ip()) {
                        return Err(format!(
                            "hostname resolves to internal/reserved address: {}",
                            addr.ip()
                        ));
                    }
                }
                
                // Return the first public address for connection pinning
                Ok(addrs[0].ip())
            }
            Err(_) => {
                // DNS resolution failed; treat as a potential SSRF risk
                Err("could not resolve hostname".to_string())
            }
        }
    } else {
        Err("url must have a host".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_ipv4_mapped_ipv6_loopback() {
        assert!(validate_webhook_url("http://[::ffff:127.0.0.1]/hook").is_err());
        assert!(validate_webhook_url("http://[::ffff:127.0.0.2]/hook").is_err());
    }

    #[test]
    fn rejects_ipv4_mapped_ipv6_metadata() {
        assert!(validate_webhook_url("http://[::ffff:169.254.169.254]/hook").is_err());
    }

    #[test]
    fn rejects_ipv4_mapped_ipv6_private() {
        assert!(validate_webhook_url("http://[::ffff:10.0.0.1]/hook").is_err());
        assert!(validate_webhook_url("http://[::ffff:192.168.1.1]/hook").is_err());
        assert!(validate_webhook_url("http://[::ffff:172.16.0.1]/hook").is_err());
    }

    #[test]
    fn rejects_nat64_wellknown() {
        assert!(validate_webhook_url("http://[64:ff9b::a9fe:a9fe]/hook").is_err());
        assert!(validate_webhook_url("http://[64:ff9b::7f00:1]/hook").is_err());
    }

    #[test]
    fn rejects_6to4() {
        assert!(validate_webhook_url("http://[2002:a9fe:a9fe::1]/hook").is_err());
    }

    #[test]
    fn rejects_ipv4_compatible_ipv6() {
        assert!(validate_webhook_url("http://[::127.0.0.1]/hook").is_err());
        assert!(validate_webhook_url("http://[::10.0.0.1]/hook").is_err());
    }

    #[test]
    fn rejects_benchmarking_ranges() {
        assert!(validate_webhook_url("http://198.18.0.1/hook").is_err());
        assert!(validate_webhook_url("http://198.19.255.255/hook").is_err());
    }

    #[test]
    fn rejects_reserved_future_use() {
        assert!(validate_webhook_url("http://240.0.0.1/hook").is_err());
        assert!(validate_webhook_url("http://255.255.255.254/hook").is_err());
    }

    #[test]
    fn rejects_loopback_ips() {
        assert!(validate_webhook_url("http://127.0.0.1/hook").is_err());
        assert!(validate_webhook_url("http://127.0.0.2/hook").is_err());
        assert!(validate_webhook_url("http://[::1]/hook").is_err());
    }

    #[test]
    fn rejects_localhost_hostname() {
        assert!(validate_webhook_url("http://localhost/hook").is_err());
        assert!(validate_webhook_url("http://LOCALHOST/hook").is_err());
    }

    #[test]
    fn rejects_private_ips() {
        assert!(validate_webhook_url("http://10.0.0.1/hook").is_err());
        assert!(validate_webhook_url("http://172.16.0.1/hook").is_err());
        assert!(validate_webhook_url("http://192.168.1.1/hook").is_err());
        assert!(validate_webhook_url("http://[fc00::1]/hook").is_err());
    }

    #[test]
    fn rejects_link_local_ips() {
        assert!(validate_webhook_url("http://169.254.0.1/hook").is_err());
        assert!(validate_webhook_url("http://[fe80::1]/hook").is_err());
    }

    #[test]
    fn rejects_multicast_ips() {
        assert!(validate_webhook_url("http://224.0.0.1/hook").is_err());
        assert!(validate_webhook_url("http://[ff00::1]/hook").is_err());
    }

    #[test]
    fn rejects_aws_metadata_endpoint() {
        assert!(validate_webhook_url("http://169.254.169.254/latest/meta-data/").is_err());
    }

    #[test]
    fn rejects_kubernetes_metadata_endpoint() {
        assert!(validate_webhook_url("http://10.0.0.1:10250/api/v1/nodes").is_err());
    }

    #[test]
    fn accepts_public_urls() {
        assert!(validate_webhook_url("https://example.com/webhook").is_ok());
        assert!(validate_webhook_url("https://api.example.com:8080/hook").is_ok());
        assert!(validate_webhook_url("http://8.8.8.8/webhook").is_ok());
    }

    #[test]
    fn rejects_invalid_scheme() {
        assert!(validate_webhook_url("ftp://example.com/hook").is_err());
    }

    #[test]
    fn rejects_no_host() {
        assert!(validate_webhook_url("http:///hook").is_err());
    }
}
