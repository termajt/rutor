use std::{fmt, io};

use bytes::Bytes;

use crate::bitfield::Bitfield;

pub mod announce;
pub mod bencode;
pub mod bitfield;
pub mod bytespeed;
pub mod disk;
pub mod engine;
pub mod net;
pub mod peer;
pub mod pending_requests;
pub mod picker;
pub mod rate_limiter;
pub mod torrent;

pub const BLOCK_SIZE: usize = 16 * 1024;

/// Represents a message exchanged between BitTorrent peers
/// according to the standard peer wire protocol.
///
/// Each variant corresponds to one of the standard messages a peer
/// can send. Some messages carry additional data (like piece index
/// or a block of data), while others are simple notifications.
#[derive(Debug, Clone)]
pub enum PeerMessage {
    /// Tells the receiving peer that it is **choked**.
    /// No data will be sent until an `Unchoke` message is received.
    Choke,

    /// Tells the receiving peer that it is **unchoked** and may
    /// request pieces.
    Unchoke,

    /// Indicates that the sending peer is **interested** in downloading pieces.
    Interested,

    /// Indicates that the sending peer is **not interested** in downloading pieces.
    NotInterested,

    /// Announces that the sending peer has successfully downloaded
    /// the piece at `piece_index`.
    ///
    /// # Fields
    ///
    /// * `piece_index` - The index of the piece that has been downloaded.
    Have(u32),

    /// Sends the bitfield of the pieces the sending peer has.
    ///
    /// # Fields
    ///    /// * `bitfield` - A `Bitfield` struct representing which pieces
    ///   the peer has.
    Bitfield(Bitfield),

    /// Requests a block of data from the receiving peer.
    ///
    /// # Fields
    ///
    /// * `index` - Piece index being requested.
    /// * `begin` - Offset within the piece.
    /// * `length` - Length of the requested block in bytes.
    Request((u32, u32, u32)),

    /// Sends a block of data in response to a `Request` message.
    ///
    /// # Fields
    ///
    /// * `index` - Piece index of the block.
    /// * `begin` - Offset within the piece.
    /// * `block` - The actual bytes of data being sent.
    Piece((u32, u32, Bytes)),

    /// Cancels a previously sent `Request`.
    ///
    /// # Fields
    ///
    /// * `index` - Piece index of the canceled block.
    /// * `begin` - Offset within the piece.
    /// * `length` - Length of the canceled block in bytes.
    Cancel((u32, u32, u32)),

    /// Announces the DHT listening port of the sending peer.
    ///
    /// # Fields
    ///
    /// * `port` - The port number the peer is listening on for DHT messages.
    Port(u16),

    KeepAlive,
}

impl fmt::Display for PeerMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PeerMessage::Bitfield(bitfield) => {
                write!(f, "PeerMessage::Bitfield({:?})", bitfield)
            }
            PeerMessage::Choke => write!(f, "PeerMessage::Choke"),
            PeerMessage::Unchoke => write!(f, "PeerMessage::Unchoke"),
            PeerMessage::Interested => write!(f, "PeerMessage::Interested"),
            PeerMessage::NotInterested => write!(f, "PeerMessage::NotInterested"),
            PeerMessage::Have(i) => write!(f, "PeerMessage::Have({})", i),
            PeerMessage::Request((i, b, l)) => {
                write!(f, "PeerMessage::Request({}, {}, {})", i, b, l)
            }
            PeerMessage::Piece((i, b, d)) => {
                write!(f, "PeerMessage::Piece({}, {}, {})", i, b, d.len())
            }
            PeerMessage::Cancel((i, b, l)) => write!(f, "PeerMessage::Cancel({}, {}, {})", i, b, l),
            PeerMessage::Port(p) => write!(f, "PeerMessage::Port({})", p),
            PeerMessage::KeepAlive => write!(f, "PeerMessage::KeepAlive"),
        }
    }
}

impl PeerMessage {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            PeerMessage::Choke => Self::encode_simple(0),
            PeerMessage::Unchoke => Self::encode_simple(1),
            PeerMessage::Interested => Self::encode_simple(2),
            PeerMessage::NotInterested => Self::encode_simple(3),
            PeerMessage::Have(index) => {
                let mut buf = Vec::with_capacity(9);
                buf.extend_from_slice(&5u32.to_be_bytes());
                buf.push(4);
                buf.extend_from_slice(&index.to_be_bytes());
                buf
            }
            PeerMessage::Bitfield(bitfield) => {
                let bytes = bitfield.as_bytes();
                let total_len = 1 + bytes.len();
                let mut buf = Vec::with_capacity(4 + total_len);
                buf.extend_from_slice(&(total_len as u32).to_be_bytes());
                buf.push(5);
                buf.extend_from_slice(bytes);
                buf
            }
            PeerMessage::Request((index, begin, length))
            | PeerMessage::Cancel((index, begin, length)) => {
                let id = if matches!(self, PeerMessage::Request(_)) {
                    6
                } else {
                    8
                };
                let mut buf = Vec::with_capacity(17);
                buf.extend_from_slice(&13u32.to_be_bytes());
                buf.push(id);
                buf.extend_from_slice(&index.to_be_bytes());
                buf.extend_from_slice(&begin.to_be_bytes());
                buf.extend_from_slice(&length.to_be_bytes());
                buf
            }
            PeerMessage::Piece((index, begin, block)) => {
                let total_len = 1 + 8 + block.len();
                let mut buf = Vec::with_capacity(4 + total_len);
                buf.extend_from_slice(&(total_len as u32).to_be_bytes());
                buf.push(7);
                buf.extend_from_slice(&index.to_be_bytes());
                buf.extend_from_slice(&begin.to_be_bytes());
                buf.extend_from_slice(block);
                buf
            }
            PeerMessage::Port(port) => {
                let mut buf = Vec::with_capacity(7);
                buf.extend_from_slice(&3u32.to_be_bytes());
                buf.push(9);
                buf.extend_from_slice(&port.to_be_bytes());
                buf
            }
            PeerMessage::KeepAlive => 0u32.to_be_bytes().to_vec(),
        }
    }

    fn encode_simple(id: u8) -> Vec<u8> {
        vec![0, 0, 0, 1, id]
    }

    pub fn parse(
        payload: &[u8],
        total_pieces: usize,
    ) -> Result<PeerMessage, Box<dyn std::error::Error>> {
        if payload.is_empty() {
            return Ok(PeerMessage::KeepAlive);
        }

        let id = payload[0];
        let data = &payload[1..];

        let msg = match id {
            0 => PeerMessage::Choke,
            1 => PeerMessage::Unchoke,
            2 => PeerMessage::Interested,
            3 => PeerMessage::NotInterested,
            4 => PeerMessage::Have(u32::from_be_bytes(data[..4].try_into()?)),
            5 => {
                if data.len() != (total_pieces + 7) / 8 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid bitfield length",
                    )
                    .into());
                }
                PeerMessage::Bitfield(Bitfield::from_bytes(data.to_vec(), total_pieces))
            }
            6 => {
                let index = u32::from_be_bytes(data[0..4].try_into()?);
                let begin = u32::from_be_bytes(data[4..8].try_into()?);
                let length = u32::from_be_bytes(data[8..12].try_into()?);
                PeerMessage::Request((index, begin, length))
            }
            7 => {
                let index = u32::from_be_bytes(data[0..4].try_into()?);
                let begin = u32::from_be_bytes(data[4..8].try_into()?);
                let block = data[8..].to_vec();
                PeerMessage::Piece((index, begin, Bytes::from(block)))
            }
            8 => {
                let index = u32::from_be_bytes(data[0..4].try_into()?);
                let begin = u32::from_be_bytes(data[4..8].try_into()?);
                let length = u32::from_be_bytes(data[8..12].try_into()?);
                PeerMessage::Cancel((index, begin, length))
            }
            9 => PeerMessage::Port(u16::from_be_bytes(data[0..2].try_into()?)),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unknown message id: {}", id),
                )
                .into());
            }
        };

        Ok(msg)
    }
}
