use rutor::bencode::{self, Bencode};
use std::collections::BTreeMap;

#[test]
fn encode_int() {
    assert_eq!(bencode::encode(&Bencode::Int(42)), b"i42e".to_vec());
    assert_eq!(bencode::encode(&Bencode::Int(-3)), b"i-3e".to_vec());
    assert_eq!(bencode::encode(&Bencode::Int(0)), b"i0e".to_vec());
}

#[test]
fn encode_bytes() {
    assert_eq!(bencode::encode(&Bencode::Bytes(b"spam".to_vec())), b"4:spam".to_vec());
    assert_eq!(bencode::encode(&Bencode::Bytes(vec![])), b"0:".to_vec());
}

#[test]
fn encode_list() {
    let v = Bencode::List(vec![Bencode::Bytes(b"spam".to_vec()), Bencode::Int(42)]);
    assert_eq!(bencode::encode(&v), b"l4:spami42ee".to_vec());
}

#[test]
fn encode_dict() {
    let mut m = BTreeMap::new();
    m.insert(b"cow".to_vec(), Bencode::Bytes(b"moo".to_vec()));
    m.insert(b"spam".to_vec(), Bencode::Bytes(b"eggs".to_vec()));
    let d = Bencode::Dict(m);
    assert_eq!(bencode::encode(&d), b"d3:cow3:moo4:spam4:eggse".to_vec());
}

#[test]
fn decode_int() {
    assert_eq!(bencode::decode(b"i42e").unwrap(), Bencode::Int(42));
    assert_eq!(bencode::decode(b"i0e").unwrap(), Bencode::Int(0));
    assert_eq!(bencode::decode(b"i-3e").unwrap(), Bencode::Int(-3));
}

#[test]
fn decode_bytes() {
    assert_eq!(bencode::decode(b"4:spam").unwrap(), Bencode::Bytes(b"spam".to_vec()));
    assert_eq!(bencode::decode(b"0:").unwrap(), Bencode::Bytes(vec![]));
}

#[test]
fn decode_list() {
    assert_eq!(bencode::decode(b"l4:spami42ee").unwrap(), Bencode::List(vec![Bencode::Bytes(b"spam".to_vec()), Bencode::Int(42)]));
}

#[test]
fn decode_dict() {
    let mut m = BTreeMap::new();
    m.insert(b"cow".to_vec(), Bencode::Bytes(b"moo".to_vec()));
    m.insert(b"spam".to_vec(), Bencode::Bytes(b"eggs".to_vec()));
    assert_eq!(bencode::decode(b"d3:cow3:moo4:spam4:eggse").unwrap(), Bencode::Dict(m));
}

#[test]
fn roundtrip_complex() {
    let mut inner = BTreeMap::new();
    inner.insert(b"k".to_vec(), Bencode::Int(10));
    let value = Bencode::List(vec![
        Bencode::Bytes(b"hello".to_vec()),
        Bencode::Dict(inner),
        Bencode::Int(-1)
    ]);
    let enc = bencode::encode(&value);
    let dec = bencode::decode(&enc).unwrap();
    assert_eq!(dec, value);
}

#[test]
fn reject_leading_zero_in_int() {
    assert!(bencode::decode(b"i01e").is_err());
}

#[test]
fn reject_leading_zero_in_length() {
    assert!(bencode::decode(b"01:ab").is_err());
}
