use std::{fmt, io};

use bytes::{BufMut, Bytes, BytesMut};

use crate::bitfield::Bitfield;

pub mod announce;
pub mod bencode;
pub mod bitfield;
pub mod bytespeed;
pub mod disk;
pub mod engine;
pub mod net;
pub mod peer;
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
    pub fn encode(&self) -> Bytes {
        let buf = match self {
            PeerMessage::Choke => Self::simple(0),
            PeerMessage::Unchoke => Self::simple(1),
            PeerMessage::Interested => Self::simple(2),
            PeerMessage::NotInterested => Self::simple(3),
            PeerMessage::Have(index) => {
                let mut b = BytesMut::with_capacity(9);
                b.put_u32(5);
                b.put_u8(4);
                b.put_u32(*index);
                b
            }
            PeerMessage::Bitfield(bitfield) => {
                let bytes = bitfield.freeze();
                let total_len = 1 + bytes.len();
                let mut b = BytesMut::with_capacity(4 + total_len);
                b.put_u32(total_len as u32);
                b.put_u8(5);
                b.extend_from_slice(&bytes);
                b
            }
            PeerMessage::Request((index, begin, length)) => {
                let mut b = BytesMut::with_capacity(17);
                b.put_u32(13);
                b.put_u8(6);
                b.put_u32(*index);
                b.put_u32(*begin);
                b.put_u32(*length);
                b
            }
            PeerMessage::Piece((index, begin, block)) => {
                let total_len = 1 + 8 + block.len();
                let mut b = BytesMut::with_capacity(4 + total_len);
                b.put_u32(total_len as u32);
                b.put_u8(7);
                b.put_u32(*index);
                b.put_u32(*begin);
                b.unsplit(block.clone().into());
                b
            }
            PeerMessage::Port(port) => {
                let mut b = BytesMut::with_capacity(7);
                b.put_u32(3);
                b.put_u8(9);
                b.put_u16(*port);
                b
            }
            PeerMessage::KeepAlive => {
                let mut b = BytesMut::with_capacity(4);
                b.put_u32(0);
                b
            }
            PeerMessage::Cancel((index, begin, length)) => {
                let mut b = BytesMut::with_capacity(17);
                b.put_u32(13);
                b.put_u8(8);
                b.put_u32(*index);
                b.put_u32(*begin);
                b.put_u32(*length);
                b
            }
        };

        buf.freeze()
    }

    fn simple(id: u8) -> BytesMut {
        let mut b = BytesMut::with_capacity(5);
        b.put_u32(1);
        b.put_u8(id);
        b
    }

    pub fn parse(
        payload: Bytes,
        total_pieces: usize,
    ) -> Result<PeerMessage, Box<dyn std::error::Error>> {
        if payload.is_empty() {
            return Ok(PeerMessage::KeepAlive);
        }

        let id = payload[0];
        let data = payload.slice(1..);

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
                PeerMessage::Bitfield(Bitfield::from_bytes(data, total_pieces))
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
