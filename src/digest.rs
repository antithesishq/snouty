//! SHA-256 digests, as image references, manifests and image stores name
//! content.

use std::fmt;
use std::str::FromStr;

use color_eyre::eyre::{Result, bail, ensure, eyre};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

const PREFIX: &str = "sha256:";

/// A `sha256:` digest, stored as lowercase hex.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Sha256Digest(String);

impl Sha256Digest {
    /// The digest of `bytes`.
    pub fn of(bytes: &[u8]) -> Self {
        Self::from_hasher(Sha256::new_with_prefix(bytes))
    }

    /// The digest of everything `hasher` has seen.
    pub fn from_hasher(hasher: Sha256) -> Self {
        Self(hex(&hasher.finalize()))
    }

    /// The content ID that `image inspect` reports: Docker writes it as a
    /// digest, Podman as bare hex.
    pub fn from_image_id(value: &str) -> Result<Self> {
        let hex = value.strip_prefix(PREFIX).unwrap_or(value);
        if !is_hex(hex) {
            bail!("image has an invalid SHA-256 content ID");
        }
        Ok(Self(hex.to_ascii_lowercase()))
    }

    /// The hex digits, without `sha256:`.
    pub fn hex(&self) -> &str {
        &self.0
    }
}

fn is_hex(hex: &str) -> bool {
    hex.len() == 64 && hex.bytes().all(|b| b.is_ascii_hexdigit())
}

impl FromStr for Sha256Digest {
    type Err = color_eyre::Report;

    fn from_str(value: &str) -> Result<Self> {
        let hex = value
            .strip_prefix(PREFIX)
            .ok_or_else(|| eyre!("unsupported digest {value}"))?;
        ensure!(is_hex(hex), "invalid digest {value}");
        Ok(Self(hex.to_ascii_lowercase()))
    }
}

impl<'de> Deserialize<'de> for Sha256Digest {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

impl Serialize for Sha256Digest {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl fmt::Display for Sha256Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{PREFIX}{}", self.0)
    }
}

fn hex(bytes: &[u8]) -> String {
    use fmt::Write as _;
    bytes.iter().fold(String::new(), |mut out, byte| {
        write!(out, "{byte:02x}").expect("writing to a String cannot fail");
        out
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use hegel::generators;
    use std::path::Path;

    const ID: &str = "afddb59f8e828f0306d47b7084673f16112c6af52cce6b6e1d3f8c737e1a984b";

    #[hegel::test]
    fn image_ids_normalize_to_one_digest(tc: hegel::TestCase) {
        let bytes = tc.draw(generators::binary().min_size(32).max_size(32));
        let digest = hex(&bytes);
        let id = Sha256Digest::from_image_id(&digest).unwrap();
        assert_eq!(
            Sha256Digest::from_image_id(&format!("sha256:{}", digest.to_ascii_uppercase()))
                .unwrap(),
            id
        );
        assert_eq!(id.to_string(), format!("sha256:{digest}"));
        assert_eq!(id.to_string().parse::<Sha256Digest>().unwrap(), id);
        let path = Path::new("cache").join(id.to_string());
        assert_eq!(path.parent(), Some(Path::new("cache")));
    }

    #[hegel::test]
    fn arbitrary_image_ids_cannot_escape_a_directory(tc: hegel::TestCase) {
        let input = tc.draw(generators::text());
        assert!(Sha256Digest::from_image_id(&format!("../{input}")).is_err());
        assert!(Sha256Digest::from_image_id(&format!("{ID}/{input}")).is_err());
    }

    #[test]
    fn a_digest_names_its_algorithm() {
        assert!(ID.parse::<Sha256Digest>().is_err());
        assert!(format!("sha512:{ID}").parse::<Sha256Digest>().is_err());
        assert_eq!(
            format!("sha256:{ID}")
                .parse::<Sha256Digest>()
                .unwrap()
                .hex(),
            ID
        );
    }
}
