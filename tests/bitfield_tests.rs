use rutor::bitfield::Bitfield;

#[test]
fn test_new_bitfield() {
    let bf = Bitfield::new(10);
    for i in 0..10 {
        assert!(!bf.get(&i));
    }
    assert_eq!(bf.as_bytes().len(), 2);
}

#[test]
fn test_set_and_get() {
    let mut bf = Bitfield::new(10);

    bf.set(&0, true);
    bf.set(&3, true);
    bf.set(&9, true);

    assert!(bf.get(&0));
    assert!(bf.get(&3));
    assert!(bf.get(&9));

    assert!(!bf.get(&1));
    assert!(!bf.get(&2));
    assert!(!bf.get(&8));
}

#[test]
fn test_clear_bit() {
    let mut bf = Bitfield::new(8);

    bf.set(&5, true);
    assert!(bf.get(&5));

    bf.set(&5, false);
    assert!(!bf.get(&5));
}

#[test]
fn test_as_bytes() {
    let mut bf = Bitfield::new(10);
    bf.set(&0, true);
    bf.set(&3, true);
    bf.set(&9, true);

    let bytes = bf.as_bytes();
    // Expect first byte: 10010000 = 0x90, second byte: 10000000 = 0x80
    assert_eq!(bytes[0], 0b10010000);
    assert_eq!(bytes[1], 0b01000000);
}

#[test]
fn test_from_bytes() {
    let bytes = vec![0b10101010, 0b11000000];
    let bf = Bitfield::from_bytes(bytes.clone(), 10);

    // Check specific bits
    assert!(bf.get(&0));
    assert!(!bf.get(&1));
    assert!(bf.get(&2));
    assert!(!bf.get(&3));
    assert!(bf.get(&4));
    assert!(!bf.get(&5));
    assert!(bf.get(&6));
    assert!(!bf.get(&7));
    assert!(bf.get(&8));
    assert!(bf.get(&9));

    // Check that bytes are preserved
    assert_eq!(bf.as_bytes(), &bytes);
}

#[test]
#[should_panic]
fn test_set_out_of_bounds() {
    let mut bf = Bitfield::new(5);
    bf.set(&5, true); // index 5 is out of range
}

#[test]
#[should_panic]
fn test_get_out_of_bounds() {
    let bf = Bitfield::new(5);
    bf.get(&5); // index 5 is out of range
}
