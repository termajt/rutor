use std::collections::BTreeMap;
use std::fmt;
use std::io::Read;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Bencode {
    Int(i64),
    Bytes(Vec<u8>),
    List(Vec<Bencode>),
    Dict(BTreeMap<Vec<u8>, Bencode>),
}

#[derive(Debug)]
pub enum Error {
    UnexpectedEof,
    UnexpectedByte(u8, usize),
    InvalidInteger(String),
    LeadingZero(usize),
    TrailingData(usize),
    InvalidDictKey(usize),
    ParseIntError(std::num::ParseIntError),
    Other(String),
}

impl From<std::num::ParseIntError> for Error {
    fn from(value: std::num::ParseIntError) -> Self {
        Error::ParseIntError(value)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::UnexpectedEof => write!(f, "unexpected end of file"),
            Error::UnexpectedByte(b, pos) => write!(f, "unexpected byte 0x{:02x} at {}", b, pos),
            Error::InvalidInteger(s) => write!(f, "invalid integer: {}", s),
            Error::LeadingZero(pos) => write!(f, "leading zero at {}", pos),
            Error::TrailingData(pos) => write!(f, "trailing data after parse at {}", pos),
            Error::InvalidDictKey(pos) => write!(f, "invalid dict key at {}", pos),
            Error::ParseIntError(e) => write!(f, "parse int error: {}", e),
            Error::Other(s) => write!(f, "{}", s),
        }
    }
}

impl std::error::Error for Error {}

type Result<T> = std::result::Result<T, Error>;

pub fn encode(value: &Bencode) -> Vec<u8> {
    let mut out = Vec::new();
    encode_to(value, &mut out);
    out
}

fn encode_to(value: &Bencode, out: &mut Vec<u8>) {
    match value {
        Bencode::Int(i) => {
            out.push(b'i');
            out.extend_from_slice(i.to_string().as_bytes());
            out.push(b'e');
        }
        Bencode::Bytes(bytes) => {
            out.extend_from_slice(bytes.len().to_string().as_bytes());
            out.push(b':');
            out.extend_from_slice(bytes);
        }
        Bencode::List(items) => {
            out.push(b'l');
            for it in items {
                encode_to(it, out);
            }
            out.push(b'e');
        }
        Bencode::Dict(map) => {
            out.push(b'd');
            for (k, v) in map.iter() {
                out.extend_from_slice(k.len().to_string().as_bytes());
                out.push(b':');
                out.extend_from_slice(k);
                encode_to(v, out);
            }
            out.push(b'e');
        }
    }
}

pub fn decode(bytes: &[u8]) -> Result<Bencode> {
    let mut p = Parser {
        data: bytes,
        pos: 0,
    };
    let v = p.parse_value()?;
    if p.pos != bytes.len() {
        return Err(Error::TrailingData(p.pos));
    }
    Ok(v)
}

struct Parser<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Result<u8> {
        self.data.get(self.pos).copied().ok_or(Error::UnexpectedEof)
    }

    fn next(&mut self) -> Result<u8> {
        let b = self.peek()?;
        self.pos += 1;
        Ok(b)
    }

    fn parse_value(&mut self) -> Result<Bencode> {
        match self.peek()? {
            b'i' => self.parse_int(),
            b'l' => self.parse_list(),
            b'd' => self.parse_dict(),
            b'0'..=b'9' => self.parse_bytes(),
            other => Err(Error::UnexpectedByte(other, self.pos)),
        }
    }

    fn parse_int(&mut self) -> Result<Bencode> {
        assert_eq!(self.next()?, b'i');
        let mut end = self.pos;
        while end < self.data.len() && self.data[end] != b'e' {
            end += 1;
        }
        if end >= self.data.len() {
            return Err(Error::UnexpectedEof);
        }
        let slice = &self.data[self.pos..end];
        if slice.is_empty() {
            return Err(Error::InvalidInteger(String::from("empty")));
        }
        if slice[0] == b'-' {
            if slice.len() >= 2 && slice[1] == b'0' {
                return Err(Error::LeadingZero(self.pos));
            }
        } else if slice[0] == b'0' && slice.len() > 1 {
            return Err(Error::LeadingZero(self.pos));
        }
        let s = std::str::from_utf8(slice)
            .map_err(|_| Error::InvalidInteger(String::from("not utf8")))?;
        let val = s.parse::<i64>().map_err(Error::from)?;
        self.pos = end + 1;
        Ok(Bencode::Int(val))
    }

    fn parse_list(&mut self) -> Result<Bencode> {
        assert_eq!(self.next()?, b'l');
        let mut items = Vec::new();
        while self.peek()? != b'e' {
            items.push(self.parse_value()?);
        }
        assert_eq!(self.next()?, b'e');
        Ok(Bencode::List(items))
    }

    fn parse_bytes(&mut self) -> Result<Bencode> {
        let len_start = self.pos;
        let mut colon = self.pos;
        while colon < self.data.len() && self.data[colon] != b':' {
            if !(b'0'..=b'9').contains(&self.data[colon]) {
                return Err(Error::UnexpectedByte(self.data[colon], colon));
            }
            colon += 1;
        }
        if colon >= self.data.len() {
            return Err(Error::UnexpectedEof);
        }
        let len_slice = &self.data[len_start..colon];
        if len_slice.is_empty() {
            return Err(Error::InvalidInteger(String::from("empty length")));
        }
        if len_slice[0] == b'0' && len_slice.len() > 1 {
            return Err(Error::LeadingZero(len_start));
        }
        let len_str = std::str::from_utf8(len_slice)
            .map_err(|_| Error::InvalidInteger(String::from("not utf8")))?;
        let len = len_str.parse::<usize>().map_err(Error::from)?;
        self.pos = colon + 1;
        if self.pos + len > self.data.len() {
            return Err(Error::UnexpectedEof);
        }
        let bytes = self.data[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(Bencode::Bytes(bytes))
    }

    fn parse_dict(&mut self) -> Result<Bencode> {
        assert_eq!(self.next()?, b'd');
        let mut map = BTreeMap::new();
        while self.peek()? != b'e' {
            let key = match self.parse_bytes()? {
                Bencode::Bytes(k) => k,
                _ => return Err(Error::InvalidDictKey(self.pos)),
            };
            let val = self.parse_value()?;
            map.insert(key, val);
        }
        assert_eq!(self.next()?, b'e');
        Ok(Bencode::Dict(map))
    }
}

pub fn decode_from_reader<R: Read>(mut reader: R) -> Result<Bencode> {
    let mut parser = StreamParser {
        reader: &mut reader,
        buf: Vec::new(),
        pos: 0,
    };
    parser.parse_value()
}

struct StreamParser<'a, R: Read> {
    reader: &'a mut R,
    buf: Vec<u8>,
    pos: usize,
}

impl<'a, R: Read> StreamParser<'a, R> {
    fn peek(&mut self) -> Result<u8> {
        if self.pos >= self.buf.len() {
            let mut byte = [0u8; 1];
            let n = self
                .reader
                .read(&mut byte)
                .map_err(|e| Error::Other(e.to_string()))?;
            if n == 0 {
                return Err(Error::UnexpectedEof);
            }
            self.buf.push(byte[0]);
        }
        Ok(self.buf[self.pos])
    }

    fn next(&mut self) -> Result<u8> {
        let b = self.peek()?;
        self.pos += 1;
        Ok(b)
    }

    fn parse_value(&mut self) -> Result<Bencode> {
        match self.peek()? {
            b'i' => self.parse_int(),
            b'l' => self.parse_list(),
            b'd' => self.parse_dict(),
            b'0'..=b'9' => self.parse_bytes(),
            other => Err(Error::UnexpectedByte(other, self.pos)),
        }
    }

    fn parse_int(&mut self) -> Result<Bencode> {
        assert_eq!(self.next()?, b'i');
        let mut buf = Vec::new();
        loop {
            let b = self.next()?;
            if b == b'e' {
                break;
            }
            buf.push(b);
        }
        let s = std::str::from_utf8(&buf)
            .map_err(|_| Error::InvalidInteger(String::from("not utf8")))?;
        let val = s.parse::<i64>().map_err(Error::from)?;
        Ok(Bencode::Int(val))
    }

    fn parse_bytes(&mut self) -> Result<Bencode> {
        let mut len_buf = Vec::new();
        loop {
            let b = self.next()?;
            if b == b':' {
                break;
            }
            len_buf.push(b);
        }
        let len_str = std::str::from_utf8(&len_buf)
            .map_err(|_| Error::InvalidInteger(String::from("not utf8")))?;
        let len = len_str.parse::<usize>().map_err(Error::from)?;
        let mut data = vec![0u8; len];
        self.reader
            .read_exact(&mut data)
            .map_err(|e| Error::Other(e.to_string()))?;
        Ok(Bencode::Bytes(data))
    }

    fn parse_list(&mut self) -> Result<Bencode> {
        assert_eq!(self.next()?, b'l');
        let mut items = Vec::new();
        while self.peek()? != b'e' {
            items.push(self.parse_value()?);
        }
        assert_eq!(self.next()?, b'e');
        Ok(Bencode::List(items))
    }

    fn parse_dict(&mut self) -> Result<Bencode> {
        assert_eq!(self.next()?, b'd');
        let mut map = BTreeMap::new();
        while self.peek()? != b'e' {
            let key = match self.parse_bytes()? {
                Bencode::Bytes(k) => k,
                _ => return Err(Error::InvalidDictKey(self.pos)),
            };
            let value = self.parse_value()?;
            map.insert(key, value);
        }
        assert_eq!(self.next()?, b'e');
        Ok(Bencode::Dict(map))
    }
}

impl Bencode {
    pub fn to_bytes(&self) -> Vec<u8> {
        encode(self)
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            Bencode::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn int(&self) -> Result<i64> {
        if let Some(n) = self.as_int() {
            return Ok(n);
        }
        Err(Error::Other(String::from("bencode is not int")))
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Bencode::Bytes(b) => Some(b.as_slice()),
            _ => None,
        }
    }

    pub fn bytes(&self) -> Result<&[u8]> {
        if let Some(bytes) = self.as_bytes() {
            return Ok(bytes);
        }
        Err(Error::Other(String::from("bencode is not bytes")))
    }

    pub fn as_dict(&self) -> Option<&BTreeMap<Vec<u8>, Bencode>> {
        match self {
            Bencode::Dict(map) => Some(map),
            _ => None,
        }
    }

    pub fn dict(&self) -> Result<&BTreeMap<Vec<u8>, Bencode>> {
        if let Some(map) = self.as_dict() {
            return Ok(map);
        }
        Err(Error::Other(String::from("bencode is not dict")))
    }

    pub fn as_list(&self) -> Option<&Vec<Bencode>> {
        match self {
            Bencode::List(list) => Some(list),
            _ => None,
        }
    }

    pub fn list(&self) -> Result<&Vec<Bencode>> {
        if let Some(list) = self.as_list() {
            return Ok(list);
        }
        Err(Error::Other(String::from("bencode is not list")))
    }
}
