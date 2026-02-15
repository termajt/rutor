use bytes::Bytes;
use rutor::{PeerMessage, bitfield::Bitfield};

fn roundtrip(msg: PeerMessage, total_pieces: usize) -> PeerMessage {
    let encoded = msg.encode();
    let payload = encoded.slice(4..);

    PeerMessage::parse(payload, total_pieces).unwrap()
}

#[test]
fn test_keepalive() {
    let msg = PeerMessage::KeepAlive;
    let encoded = msg.encode();
    assert_eq!(&encoded[..], &[0, 0, 0, 0]);
}

#[test]
fn test_simple_messages() {
    let msgs = [
        PeerMessage::Choke,
        PeerMessage::Unchoke,
        PeerMessage::Interested,
        PeerMessage::NotInterested,
    ];

    for msg in msgs {
        let decoded = roundtrip(msg.clone(), 0);
        assert_eq!(format!("{}", msg), format!("{}", decoded));
    }
}

#[test]
fn test_have() {
    let msg = PeerMessage::Have(42);
    let decoded = roundtrip(msg.clone(), 0);
    assert_eq!(format!("{}", msg), format!("{}", decoded));
}

#[test]
fn test_request() {
    let msg = PeerMessage::Request((1, 16_384, 16_384));
    let decoded = roundtrip(msg.clone(), 0);
    assert_eq!(format!("{}", msg), format!("{}", decoded));
}

#[test]
fn test_cancel() {
    let msg = PeerMessage::Cancel((3, 0, 16_384));
    let decoded = roundtrip(msg.clone(), 0);
    assert_eq!(format!("{}", msg), format!("{}", decoded));
}

#[test]
fn test_port() {
    let msg = PeerMessage::Port(6881);
    let decoded = roundtrip(msg.clone(), 0);
    assert_eq!(format!("{}", msg), format!("{}", decoded));
}

#[test]
fn test_piece() {
    let block = Bytes::from_static(b"hello world");
    let msg = PeerMessage::Piece((7, 0, block.clone()));

    let decoded = roundtrip(msg, 0);

    match decoded {
        PeerMessage::Piece((i, b, data)) => {
            assert_eq!(i, 7);
            assert_eq!(b, 0);
            assert_eq!(data, block);
        }
        _ => panic!("expected Piece"),
    }
}

#[test]
fn test_bitfield_roundtrip() {
    let total_pieces = 10;
    let mut bf = Bitfield::new(total_pieces);
    bf.set(&0, true);
    bf.set(&3, true);
    bf.set(&9, true);

    let msg = PeerMessage::Bitfield(bf.clone());
    let decoded = roundtrip(msg, total_pieces);

    match decoded {
        PeerMessage::Bitfield(decoded_bf) => {
            assert_eq!(decoded_bf.len(), bf.len());
            assert_eq!(decoded_bf.bits(), bf.bits());
        }
        _ => panic!("expected Bitfield"),
    }
}

#[test]
fn test_invalid_bitfield_length() {
    // bitfield claims 10 pieces -> needs 2 bytes
    let bad_payload = Bytes::from_static(&[
        5,    // id
        0xff, // only 1 byte instead of 2
    ]);

    let err = PeerMessage::parse(bad_payload, 10);
    assert!(err.is_err());
}

#[test]
fn test_unknown_message_id() {
    let payload = Bytes::from_static(&[99]);
    let err = PeerMessage::parse(payload, 0);
    assert!(err.is_err());
}
