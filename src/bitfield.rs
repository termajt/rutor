/// A `Bitfield` represents which pieces a BitTorrent peer has downloaded.
///
/// Each piece is represented by a single bit: `1` if the piece is available,
/// `0` if not. Bits are stored **most-significant-bit first** within each byte,
/// following the BitTorrent wire protocol specification.
#[derive(Debug, Clone)]
pub struct Bitfield {
    bits: Vec<u8>,
    length: usize,
}

impl Bitfield {
    /// Creates a new `Bitfield` of the given length (number of pieces),
    /// with all bits initialized to `false` (no pieces).
    ///
    /// # Arguments
    ///
    /// * `length` - Total number of pieces in the torrent.
    pub fn new(length: usize) -> Self {
        {
            let byte_len = (length + 7) / 8;
            Bitfield {
                bits: vec![0; byte_len],
                length: length,
            }
        }
    }

    pub fn has_any_zero(&self) -> bool {
        for i in 0..self.length {
            if !self.get(&i) {
                return true;
            }
        }

        false
    }

    pub fn is_interesting_to(&self, other: &Bitfield) -> bool {
        for i in 0..self.length {
            if self.get(&i) && !other.get(&i) {
                return true;
            }
        }
        false
    }

    /// Constructs a `Bitfield` from raw bytes and a specified number of pieces.
    ///
    /// The `bytes` vector must contain at least `(length + 7) / 8` bytes.
    ///
    /// # Arguments
    ///
    /// * `bytes` - Vector of bytes representing the bitfield.
    /// * `length` - Total number of pieces.
    ///
    /// # Panics
    ///
    /// Panics if `bytes.len()` is insufficient for the given length.
    pub fn from_bytes(bytes: Vec<u8>, length: usize) -> Self {
        assert!(bytes.len() >= (length + 7) / 8, "not enough bytes");
        Bitfield {
            bits: bytes,
            length: length,
        }
    }

    /// Sets the bit corresponding to the given piece index.
    ///
    /// # Arguments
    ///
    /// * `index` - Piece index to set.
    /// * `value` - `true` to mark as available, `false` to mark as missing.
    ///
    /// # Panics
    ///
    /// Panics if `index >= length`.
    pub fn set(&mut self, index: &usize, value: bool) {
        assert!(*index < self.length, "index out of range");
        let byte_index = index / 8;
        let bit_index = 7 - (index % 8);
        if value {
            self.bits[byte_index] |= 1 << bit_index;
        } else {
            self.bits[byte_index] &= !(1 << bit_index);
        }
    }

    /// Returns `true` if the piece at the given index is present, `false` otherwise.
    ///
    /// # Arguments
    ///
    /// * `index` - Piece index to query.
    ///
    /// # Panics
    ///
    /// Panics if `index >= length`.
    pub fn get(&self, index: &usize) -> bool {
        assert!(*index < self.length, "index out of range");
        let byte_index = index / 8;
        let bit_index = 7 - (index % 8);
        (self.bits[byte_index] & (1 << bit_index)) != 0
    }

    pub fn get_ones(&self) -> Vec<usize> {
        let mut result = Vec::new();
        for index in 0..self.length {
            let byte_index = index / 8;
            let bit_index = 7 - (index % 8);

            if (self.bits[byte_index] & (1 << bit_index)) != 0 {
                result.push(index);
            }
        }
        result
    }

    /// Returns the underlying bytes of the bitfield.
    ///
    /// Useful for sending the bitfield over the wire in BitTorrent messages.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bits
    }

    pub fn len(&self) -> usize {
        self.bits.len()
    }

    /// Merges another `Bitfield` into this one, setting bits that are set in either.
    ///
    /// # Panics
    ///
    /// Panics if the two bitfields are of different lengths.
    pub fn merge(&mut self, other: &Bitfield) {
        assert_eq!(
            self.bits.len(),
            other.bits.len(),
            "bitfields must be the same length"
        );

        for (i, byte) in other.bits.iter().enumerate() {
            self.bits[i] |= byte;
        }
    }

    pub fn merge_safe(&mut self, other: &Bitfield) {
        let min_bytes = self.bits.len().min(other.bits.len());

        for i in 0..min_bytes {
            self.bits[i] |= other.bits[i];
        }
    }

    pub fn count_ones(&self) -> usize {
        self.bits.iter().map(|b| b.count_ones() as usize).sum()
    }

    pub fn count_zeros(&self) -> usize {
        let mut zeros = 0;
        for i in 0..self.length {
            if !self.get(&i) {
                zeros += 1;
            }
        }

        zeros
    }

    pub fn has_any(&self) -> bool {
        self.bits.iter().any(|&b| b != 0)
    }

    pub fn differs_from(&self, other: &Bitfield) -> bool {
        if self.len() != other.len() {
            return true;
        }

        self.bits.iter().zip(&other.bits).any(|(a, b)| a ^ b != 0)
    }
}
