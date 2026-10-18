//! Dependency-free container healthcheck (#469).
//!
//! Every service binary accepts a `healthcheck` subcommand that performs an
//! HTTP GET against its own health endpoint on localhost and exits 0 on a 2xx
//! response, 1 otherwise. This lets `docker-compose` and the `Dockerfile`
//! `HEALTHCHECK` probe the services without `curl`/`wget`, which are not
//! installed in the slim (or distroless) runtime image.
//!
//! The probe uses only `std::net`, so it runs before any config parsing,
//! database connection or async runtime work.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::time::Duration;

/// Default timeout for connecting, writing and reading.
const TIMEOUT: Duration = Duration::from_secs(3);

/// Turn a bind address (e.g. `0.0.0.0:8080`, `[::]:8080`, `127.0.0.1:9091`)
/// into the loopback address to probe. Wildcard binds are reachable on
/// loopback; specific addresses are probed as-is.
pub fn probe_addr(bind_addr: &str) -> Option<SocketAddr> {
    let addr = bind_addr.to_socket_addrs().ok()?.next()?;
    let ip = if addr.ip().is_unspecified() {
        if addr.is_ipv4() {
            std::net::Ipv4Addr::LOCALHOST.into()
        } else {
            std::net::Ipv6Addr::LOCALHOST.into()
        }
    } else {
        addr.ip()
    };
    Some(SocketAddr::new(ip, addr.port()))
}

/// GET `path` on `bind_addr` and return whether the response status is 2xx.
pub fn check(bind_addr: &str, path: &str) -> std::result::Result<(), String> {
    let addr = probe_addr(bind_addr).ok_or_else(|| format!("invalid address {bind_addr:?}"))?;
    let mut stream =
        TcpStream::connect_timeout(&addr, TIMEOUT).map_err(|e| format!("connect {addr}: {e}"))?;
    stream.set_read_timeout(Some(TIMEOUT)).map_err(|e| e.to_string())?;
    stream.set_write_timeout(Some(TIMEOUT)).map_err(|e| e.to_string())?;

    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nUser-Agent: lumenqraph-healthcheck\r\nConnection: close\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .map_err(|e| format!("write: {e}"))?;

    // Only the status line matters; read just enough to get it.
    let mut buf = [0u8; 64];
    let mut len = 0;
    while len < buf.len() {
        match stream.read(&mut buf[len..]) {
            Ok(0) => break,
            Ok(n) => {
                len += n;
                if buf[..len].contains(&b'\n') {
                    break;
                }
            }
            Err(e) => return Err(format!("read: {e}")),
        }
    }
    let head = String::from_utf8_lossy(&buf[..len]);
    let status = parse_status(&head).ok_or_else(|| format!("malformed response: {head:?}"))?;
    if (200..300).contains(&status) {
        Ok(())
    } else {
        Err(format!("unhealthy: HTTP {status}"))
    }
}

/// Run the probe and exit the process with 0 (healthy) or 1 (unhealthy).
pub fn run_and_exit(bind_addr: &str, path: &str) -> ! {
    match check(bind_addr, path) {
        Ok(()) => std::process::exit(0),
        Err(e) => {
            eprintln!("healthcheck failed: {e}");
            std::process::exit(1)
        }
    }
}

fn parse_status(head: &str) -> Option<u16> {
    let line = head.lines().next()?;
    let mut parts = line.split_whitespace();
    let version = parts.next()?;
    if !version.starts_with("HTTP/") {
        return None;
    }
    parts.next()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wildcard_binds_probe_loopback() {
        assert_eq!(probe_addr("0.0.0.0:8080").unwrap().to_string(), "127.0.0.1:8080");
        assert_eq!(probe_addr("[::]:8080").unwrap().to_string(), "[::1]:8080");
        assert_eq!(probe_addr("127.0.0.1:9091").unwrap().to_string(), "127.0.0.1:9091");
    }

    #[test]
    fn parses_status_line() {
        assert_eq!(parse_status("HTTP/1.1 200 OK\r\n"), Some(200));
        assert_eq!(parse_status("HTTP/1.1 503 Service Unavailable\r\n"), Some(503));
        assert_eq!(parse_status("garbage"), None);
    }
}
