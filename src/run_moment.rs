use color_eyre::eyre::{Result, bail, eyre};

const TOKEN_VERSION_V1: u8 = 0x01;
const TOKEN_LEN: usize = 17;
const TICKS_PER_VTIME: u64 = 1_u64 << 32;
const MAX_TICK_ROUNDING_ERROR: f64 = 0.001;
const BASE62_ALPHABET: &[u8; 62] =
    b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunMoment {
    hash: i64,
    ticks: u64,
}

impl RunMoment {
    pub fn new(hash: i64, ticks: u64) -> Self {
        Self { hash, ticks }
    }

    pub fn from_wire(input_hash: &str, vtime: &str) -> Result<Self> {
        let hash = input_hash
            .parse::<i64>()
            .map_err(|e| eyre!("invalid input_hash: {e}"))?;
        let ticks = parse_vtime_to_ticks(vtime)?;
        Ok(Self { hash, ticks })
    }

    pub fn from_token(token: &str) -> Result<Self> {
        let token = token.trim();
        if token.is_empty() {
            bail!("invalid moment token: empty token");
        }

        let payload = decode_base62(token)?;
        if payload[0] != TOKEN_VERSION_V1 {
            bail!("invalid moment token: unsupported version {}", payload[0]);
        }

        let mut hash = [0_u8; 8];
        hash.copy_from_slice(&payload[1..9]);

        let mut ticks = [0_u8; 8];
        ticks.copy_from_slice(&payload[9..17]);

        Ok(Self {
            hash: i64::from_be_bytes(hash),
            ticks: u64::from_be_bytes(ticks),
        })
    }

    pub fn to_token(self) -> String {
        let mut payload = [0_u8; TOKEN_LEN];
        payload[0] = TOKEN_VERSION_V1;
        payload[1..9].copy_from_slice(&self.hash.to_be_bytes());
        payload[9..17].copy_from_slice(&self.ticks.to_be_bytes());
        encode_base62(&payload)
    }

    pub fn to_wire(self) -> (String, String) {
        (self.hash.to_string(), format_ticks(self.ticks))
    }

    pub fn sort_key(self) -> (u64, i64) {
        (self.ticks, self.hash)
    }
}

fn parse_vtime_to_ticks(vtime: &str) -> Result<u64> {
    let vtime = vtime.trim();
    if vtime.is_empty() {
        bail!("invalid vtime: empty value");
    }
    let vtime = vtime
        .parse::<f64>()
        .map_err(|e| eyre!("invalid vtime: {e}"))?;
    if !vtime.is_finite() || vtime < 0.0 {
        bail!("invalid vtime: expected a non-negative finite number");
    }

    let scaled = vtime * TICKS_PER_VTIME as f64;
    if scaled > u64::MAX as f64 {
        bail!("invalid vtime: value exceeds tick range");
    }

    let rounded = scaled.round();
    if (scaled - rounded).abs() > MAX_TICK_ROUNDING_ERROR {
        bail!("invalid vtime: not close enough to an integer tick value");
    }

    Ok(rounded as u64)
}

fn format_ticks(ticks: u64) -> String {
    if ticks.is_multiple_of(TICKS_PER_VTIME) {
        return format!("{}.0", ticks / TICKS_PER_VTIME);
    }
    (ticks as f64 / TICKS_PER_VTIME as f64).to_string()
}

fn encode_base62(bytes: &[u8]) -> String {
    let mut value = bytes.to_vec();
    let mut encoded = Vec::new();

    while value.iter().any(|byte| *byte != 0) {
        let mut remainder = 0_u32;
        let mut quotient = Vec::with_capacity(value.len());
        let mut seen_non_zero = false;

        for byte in value {
            let current = (remainder << 8) | u32::from(byte);
            let digit = (current / 62) as u8;
            remainder = current % 62;

            if seen_non_zero || digit != 0 {
                quotient.push(digit);
                seen_non_zero = true;
            }
        }

        encoded.push(BASE62_ALPHABET[remainder as usize] as char);
        value = quotient;
    }

    encoded.iter().rev().collect()
}

fn decode_base62(token: &str) -> Result<[u8; TOKEN_LEN]> {
    let mut bytes = Vec::new();

    for ch in token.bytes() {
        let Some(value) = decode_base62_digit(ch) else {
            bail!(
                "invalid moment token: unsupported character '{}'",
                ch as char
            );
        };

        let mut carry = u32::from(value);
        for byte in bytes.iter_mut().rev() {
            let current = u32::from(*byte) * 62 + carry;
            *byte = (current & 0xFF) as u8;
            carry = current >> 8;
        }

        while carry > 0 {
            bytes.insert(0, (carry & 0xFF) as u8);
            carry >>= 8;
        }
    }

    if bytes.len() > TOKEN_LEN {
        bail!("invalid moment token: payload too large");
    }

    let mut payload = [0_u8; TOKEN_LEN];
    payload[TOKEN_LEN - bytes.len()..].copy_from_slice(&bytes);
    Ok(payload)
}

fn decode_base62_digit(ch: u8) -> Option<u8> {
    match ch {
        b'0'..=b'9' => Some(ch - b'0'),
        b'A'..=b'Z' => Some(ch - b'A' + 10),
        b'a'..=b'z' => Some(ch - b'a' + 36),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_round_trips() {
        let original = RunMoment::new(-123, 1_u64 << 32);
        let decoded = RunMoment::from_token(&original.to_token()).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn token_rejects_invalid_character() {
        let error = RunMoment::from_token("not-a-token!").unwrap_err();
        assert!(error.to_string().contains("unsupported character"));
    }

    #[test]
    fn token_rejects_unknown_version() {
        let error = RunMoment::from_token("0").unwrap_err();
        assert!(error.to_string().contains("unsupported version"));
    }

    #[test]
    fn from_wire_parses_decimal_vtime_exactly() {
        let moment = RunMoment::from_wire("-4735081784258020614", "311.8487535319291").unwrap();
        assert_eq!(moment.to_token(), "Da8DV943BygFz4ybuVRlDS");
    }

    #[test]
    fn from_wire_rejects_inexact_decimal_vtime() {
        let error = RunMoment::from_wire("-123", "0.1").unwrap_err();
        assert!(error.to_string().contains("not close enough"));
    }

    #[test]
    fn to_wire_formats_ticks_exactly() {
        let moment = RunMoment::new(-4735081784258020614, 1_339_380_197_718);
        let (hash, vtime) = moment.to_wire();
        assert_eq!(hash, "-4735081784258020614");
        assert_eq!(vtime, "311.8487535319291");
    }

    #[test]
    fn preserves_sample_wire_round_trip() {
        let original = RunMoment::from_wire("-456", "2.0").unwrap();
        let (hash, vtime) = original.to_wire();
        assert_eq!(hash, "-456");
        assert_eq!(vtime, "2.0");
    }

    #[test]
    fn sort_key_orders_by_ticks_then_hash() {
        let earlier = RunMoment::from_wire("-100", "5.0").unwrap();
        let later = RunMoment::from_wire("-200", "10.0").unwrap();
        assert!(earlier.sort_key() < later.sort_key());
    }
}
