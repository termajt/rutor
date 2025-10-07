use std::fmt::Write;
use std::net::UdpSocket;
use std::time::Instant;
use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4, ToSocketAddrs},
    time::Duration,
};

use rand::RngCore;
use reqwest::blocking::Client;

use crate::{
    bencode::{self, Bencode},
    torrent::Torrent,
};

/// Represents the state of a single tracker, including its URL and announce timing.
#[derive(Debug, Clone)]
pub struct TrackerState {
    pub url: String,
    pub last_announce: Option<Instant>,
    pub next_interval: Duration,
}

impl TrackerState {
    fn new(url: String) -> Self {
        TrackerState {
            url: url,
            last_announce: None,
            next_interval: Duration::from_secs(300),
        }
    }

    fn should_announce(&self) -> bool {
        match self.last_announce {
            None => true,
            Some(last) => last.elapsed() >= self.next_interval,
        }
    }

    /// Returns the remaining time until the next announce for this tracker.
    pub fn time_until_next(&self) -> Duration {
        match self.last_announce {
            None => Duration::ZERO,
            Some(last) => {
                if last.elapsed() >= self.next_interval {
                    return Duration::ZERO;
                }
                self.next_interval - last.elapsed()
            }
        }
    }

    /// Updates the tracker state from a tracker response, resetting the timer and interval.
    pub fn update_from_response(&mut self, resp: &TrackerResponse) {
        if let Some(interval) = resp.interval {
            self.next_interval = Duration::from_secs(interval as u64);
        }
    }

    pub fn announced(&mut self) {
        self.last_announce = Some(Instant::now());
    }
}

/// Represents a response from a tracker.
#[derive(Debug, Clone)]
pub struct TrackerResponse {
    /// The interval in seconds until the client should reannounce.
    pub interval: Option<u32>,
    /// The minimum interval requested by the tracker./// The minimum interval requested by the tracker.
    pub min_interval: Option<u32>,
    /// Optional tracker ID, sometimes required by private trackers.
    pub tracker_id: Option<String>,
    /// Number of seeders.
    pub complete: Option<u32>,
    /// Number of leechers.
    pub incomplete: Option<u32>,
    /// List of peers returned by the tracker.
    pub peers: Vec<SocketAddr>,
}

impl TrackerResponse {
    /// Constructs a `TrackerResponse` from a bencoded dictionary.
    ///
    /// Returns an error if the bencode is invalid or peers cannot be parsed.
    fn from_bencode(b: &Bencode) -> Result<Self, Box<dyn std::error::Error>> {
        let dict = b.dict()?;
        let interval = match dict.get(&b"interval".to_vec()) {
            Some(binterval) => Some(binterval.int()? as u32),
            _ => None,
        };
        let min_interval = match dict.get(&b"min interval".to_vec()) {
            Some(bmin_interval) => Some(bmin_interval.int()? as u32),
            _ => None,
        };
        let tracker_id = match dict.get(&b"tracker id".to_vec()) {
            Some(btracker_id) => Some(String::from_utf8_lossy(btracker_id.bytes()?).to_string()),
            _ => None,
        };
        let complete = match dict.get(&b"complete".to_vec()) {
            Some(bcomplete) => Some(bcomplete.int()? as u32),
            _ => None,
        };
        let incomplete = match dict.get(&b"incomplete".to_vec()) {
            Some(bincomplete) => Some(bincomplete.int()? as u32),
            _ => None,
        };
        let peers = match dict.get(&b"peers".to_vec()) {
            Some(Bencode::Bytes(bytes)) => parse_compact_peers(bytes)?,
            Some(Bencode::List(list)) => parse_dict_peers(list)?,
            other => return Err(format!("invalid 'peers' bencode type {:?}", other).into()),
        };
        Ok(TrackerResponse {
            interval: interval,
            min_interval: min_interval,
            tracker_id: tracker_id,
            complete: complete,
            incomplete: incomplete,
            peers: peers,
        })
    }
}

/// Manages announces for all tiers of trackers.
#[derive(Debug)]
pub struct AnnounceManager {
    /// A list of tracker tiers. Each tier is a list of trackers.
    pub tiers: Vec<Vec<TrackerState>>,
}

impl AnnounceManager {
    /// Creates a new `AnnounceManager` for a given torrent.
    ///
    /// If the torrent has an `announce_list`, it is used; otherwise, the single `announce` URL is used.
    pub fn new(torrent: &Torrent) -> Self {
        let mut tiers = Vec::new();

        if !torrent.announce_list.is_empty() {
            for tier_urls in &torrent.announce_list {
                let tier = tier_urls
                    .iter()
                    .map(|url| TrackerState::new(url.clone()))
                    .collect();
                tiers.push(tier);
            }
        } else if let Some(url) = &torrent.announce {
            tiers.push(vec![TrackerState::new(url.clone())]);
        }

        AnnounceManager { tiers: tiers }
    }

    /// Returns a mutable reference to the next tracker that is due for announcing.
    ///
    /// Returns `None` if no trackers are ready yet.
    pub fn next_due_tracker(&mut self) -> Option<&mut TrackerState> {
        for tier in &mut self.tiers {
            for tracker in tier {
                if tracker.should_announce() {
                    return Some(tracker);
                }
            }
        }
        None
    }
}

fn percent_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 3);
    for &b in bytes {
        if (b'0'..=b'9').contains(&b)
            || (b'a'..=b'z').contains(&b)
            || (b'A'..=b'Z').contains(&b)
            || b == b'-'
            || b == b'.'
            || b == b'_'
            || b == b'~'
        {
            out.push(b as char);
        } else {
            write!(out, "%{b:02X}").unwrap();
        }
    }
    out
}

fn parse_compact_peers(bytes: &[u8]) -> Result<Vec<SocketAddr>, Box<dyn std::error::Error>> {
    if bytes.len() % 6 != 0 {
        return Err("invalid compact peers length".into());
    }

    let mut peers = Vec::with_capacity(bytes.len() / 6);
    for chunk in bytes.chunks_exact(6) {
        let ip = Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
        let port = u16::from_be_bytes([chunk[4], chunk[5]]);
        peers.push(SocketAddr::V4(SocketAddrV4::new(ip, port)));
    }
    Ok(peers)
}

fn parse_dict_peers(list: &[Bencode]) -> Result<Vec<SocketAddr>, Box<dyn std::error::Error>> {
    let mut peers = Vec::with_capacity(list.len());
    for item in list {
        let map = match item.as_dict() {
            Some(bmap) => bmap,
            _ => continue,
        };
        match map.get(&b"ip".to_vec()) {
            Some(bip) => {
                let s = String::from_utf8_lossy(bip.bytes()?);
                s.to_socket_addrs()?.into_iter().for_each(|addr| {
                    peers.push(addr);
                });
            }
            _ => continue,
        };
    }
    Ok(peers)
}

/// Announces the client's presence and download status to a BitTorrent tracker.
///
/// This function automatically detects whether the tracker URL uses the
/// `http://` or `udp://` protocol and calls the corresponding announce
/// implementation (`announce_http` or `announce_udp`).
///
/// # Parameters
///
/// * `tracker_url` - The full tracker announce URL (e.g., `"http://tracker.example.com/announce"`
///   or `"udp://tracker.opentrackr.org:1337/announce"`).
/// * `info_hash` - The 20-byte SHA-1 hash of the torrent’s info dictionary.
/// * `peer_id` - The 20-byte unique peer identifier for this client instance.
/// * `port` - The TCP/UDP port on which this client is listening for incoming peers.
/// * `uploaded` - Total number of bytes uploaded so far.
/// * `downloaded` - Total number of bytes downloaded so far.
/// * `left` - Number of bytes left to download (i.e., remaining payload).
/// * `event` - Optional tracker event, such as `"started"`, `"stopped"`, or `"completed"`.
/// * `timeout` - Optional network timeout to apply for tracker communication.
///
/// # Returns
///
/// On success, returns a [`TrackerResponse`] containing interval, peer list, and
/// seeder/leecher counts as reported by the tracker.  
/// On failure, returns an error describing the issue.
///
/// # Errors
///
/// Returns an error if:
/// - The tracker URL has an unsupported protocol (not HTTP or UDP)
/// - The network operation fails (e.g., timeout, connection error)
/// - The tracker returns malformed or invalid data
pub fn announce(
    tracker_url: &str,
    info_hash: &[u8; 20],
    peer_id: &[u8; 20],
    port: u16,
    uploaded: u64,
    downloaded: u64,
    left: u64,
    event: Option<&str>,
    timeout: Option<Duration>,
) -> Result<TrackerResponse, Box<dyn std::error::Error>> {
    if tracker_url.starts_with("http://") {
        announce_http(
            tracker_url,
            info_hash,
            peer_id,
            port,
            uploaded,
            downloaded,
            left,
            event,
            timeout,
        )
    } else if tracker_url.starts_with("udp://") {
        announce_udp(
            tracker_url,
            info_hash,
            peer_id,
            port,
            uploaded,
            downloaded,
            left,
            event,
            timeout,
        )
    } else {
        Err("invalid tracker protocol".into())
    }
}

fn announce_http(
    tracker_url: &str,
    info_hash: &[u8; 20],
    peer_id: &[u8; 20],
    port: u16,
    uploaded: u64,
    downloaded: u64,
    left: u64,
    event: Option<&str>,
    timeout: Option<Duration>,
) -> Result<TrackerResponse, Box<dyn std::error::Error>> {
    let mut url = String::new();
    url.push_str(tracker_url);
    url.push('?');
    write!(
        &mut url,
        "info_hash={}&peer_id={}&port={}&uploaded={}&downloaded={}&left={}&compact=1",
        percent_encode(info_hash),
        percent_encode(peer_id),
        port,
        uploaded,
        downloaded,
        left
    )?;
    if let Some(ev) = event {
        url.push_str(format!("&event={}", ev).as_str());
    }
    let timeout = timeout.unwrap_or(Duration::from_secs(30));
    let client = Client::builder()
        .user_agent("Rutor/0.1")
        .timeout(timeout)
        .build()?;

    let mut response = client.get(url).send()?;

    if !response.status().is_success() {
        return Err(format!("tracker returned HTTP {}", response.status()).into());
    }

    let bencoded = bencode::decode_from_reader(&mut response)?;
    let tracker_response = TrackerResponse::from_bencode(&bencoded)?;
    Ok(tracker_response)
}

fn announce_udp(
    tracker_url: &str,
    info_hash: &[u8; 20],
    peer_id: &[u8; 20],
    port: u16,
    uploaded: u64,
    downloaded: u64,
    left: u64,
    event: Option<&str>,
    timeout: Option<Duration>,
) -> Result<TrackerResponse, Box<dyn std::error::Error>> {
    let timeout = timeout.unwrap_or(Duration::from_secs(15));
    let addr_str = tracker_url.strip_prefix("udp://").unwrap_or(tracker_url);
    let host_port = addr_str.split('/').next().unwrap_or(tracker_url);
    let tracker_addr = host_port
        .to_socket_addrs()?
        .next()
        .ok_or("invalid udp tracker address")?;
    let socket = UdpSocket::bind("0.0.0.0:0")?;

    let mut buf = Vec::with_capacity(16);
    let conn_id: u64 = 0x41727101980;
    let action_connect: u32 = 0;
    let transaction_id: u32 = rand::rng().next_u32();

    buf.extend_from_slice(&conn_id.to_be_bytes());
    buf.extend_from_slice(&action_connect.to_be_bytes());
    buf.extend_from_slice(&transaction_id.to_be_bytes());
    send_all(&mut buf, &socket, tracker_addr, timeout)?;

    let resp = recv_packet(&socket, timeout)?;
    if resp.len() < 16 {
        return Err("invalid connect response".into());
    }
    let action = u32::from_be_bytes(resp[0..4].try_into()?);
    let rx_tx_id = u32::from_be_bytes(resp[4..8].try_into()?);
    if action != 0 || rx_tx_id != transaction_id {
        return Err("invalid connect response (bad action or transaction id)".into());
    }
    let connection_id = u64::from_be_bytes(resp[8..16].try_into()?);

    let action_announce: u32 = 1;
    let transaction_id = rand::rng().next_u32();
    let event_code: u32 = match event {
        Some("completed") => 1,
        Some("started") => 2,
        Some("stopped") => 3,
        _ => 0,
    };

    let mut req = Vec::with_capacity(98);
    req.extend_from_slice(&connection_id.to_be_bytes());
    req.extend_from_slice(&action_announce.to_be_bytes());
    req.extend_from_slice(&transaction_id.to_be_bytes());
    req.extend_from_slice(info_hash);
    req.extend_from_slice(peer_id);
    req.extend_from_slice(&downloaded.to_be_bytes());
    req.extend_from_slice(&left.to_be_bytes());
    req.extend_from_slice(&uploaded.to_be_bytes());
    req.extend_from_slice(&event_code.to_be_bytes());
    req.extend_from_slice(&0u32.to_be_bytes());
    req.extend_from_slice(&rand::rng().next_u32().to_be_bytes());
    req.extend_from_slice(&(-1i32).to_be_bytes());
    req.extend_from_slice(&port.to_be_bytes());
    send_all(&mut req, &socket, tracker_addr, timeout)?;

    let resp = recv_packet(&socket, timeout)?;
    if resp.len() < 20 {
        return Err("invalid announce response".into());
    }

    let action = u32::from_be_bytes(resp[0..4].try_into()?);
    let rx_tx_id = u32::from_be_bytes(resp[4..8].try_into()?);
    if action != 1 || rx_tx_id != transaction_id {
        return Err("invalid announce response (bad action or transaction id)".into());
    }

    let interval = u32::from_be_bytes(resp[8..12].try_into()?);
    let leechers = u32::from_be_bytes(resp[12..16].try_into()?);
    let seeders = u32::from_be_bytes(resp[16..20].try_into()?);

    let peers = parse_compact_peers(&resp[20..])?;
    Ok(TrackerResponse {
        interval: Some(interval),
        min_interval: None,
        tracker_id: None,
        complete: Some(seeders),
        incomplete: Some(leechers),
        peers: peers,
    })
}

fn send_all(
    data: &mut Vec<u8>,
    socket: &UdpSocket,
    addr: SocketAddr,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    socket.set_write_timeout(Some(timeout))?;
    let _ = socket.send_to(&data, addr)?;
    Ok(())
}

fn recv_packet(
    socket: &UdpSocket,
    timeout: Duration,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    socket.set_read_timeout(Some(timeout))?;
    let mut buf = [0u8; 2048];
    let (n, _) = socket.recv_from(&mut buf)?;
    Ok(buf[..n].to_vec())
}
