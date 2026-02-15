use std::time::Duration;

use reqwest::blocking::Client;

use crate::bencode::{self, Bencode};

const HTTP_CLIENT_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug)]
pub struct MagnetLink {
    info_hash: String,
    trackers: Vec<String>,
}

impl MagnetLink {
    pub fn parse(input: &str) -> Result<Self, Box<dyn std::error::Error>> {
        if !input.starts_with("magnet:?") {
            return Err("not a magnet link".into());
        }

        let mut info_hash: Option<String> = None;
        let mut trackers = Vec::new();

        for part in input["magnet:?".len()..].split('&') {
            if let Some(v) = part.strip_prefix("xt=urn:btih:") {
                info_hash = Some(v.to_string());
            } else if let Some(v) = part.strip_prefix("tr=") {
                trackers.push(urlencoding::decode(v)?);
            }
        }

        Ok(Self {
            info_hash: info_hash.ok_or("missing info hash")?,
            trackers,
        })
    }

    pub fn fetch(&self) -> Result<Bencode, Box<dyn std::error::Error>> {
        log::info!("fetching metadata for {}", self.info_hash);
        let client = Client::builder()
            .user_agent("Rutor/0.1")
            .timeout(Some(HTTP_CLIENT_TIMEOUT))
            .build()?;

        let url = format!("https://hash2torrent.com/torrents/{}", self.info_hash);
        let response = client.get(url).send()?;

        if !response.status().is_success() {
            return Err(format!("hash2torrent returned HTTP {}", response.status()).into());
        }

        let data = response.bytes()?;
        let torrent_bencode = bencode::decode(&data)?;

        Ok(torrent_bencode)
    }

    pub fn trackers(&self) -> Vec<String> {
        self.trackers.clone()
    }
}
