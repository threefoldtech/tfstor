use super::{errors::FsError, fs::PTR_SIZE};
use chrono::Utc;
use std::{
    convert::{TryFrom, TryInto},
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug)]
pub struct BucketMeta {
    ctime: i64,
    name: String,
}

impl BucketMeta {
    pub fn new(name: String) -> Self {
        Self {
            ctime: Utc::now().timestamp(),
            name,
        }
    }

    pub fn ctime(&self) -> SystemTime {
        UNIX_EPOCH + std::time::Duration::from_secs(self.ctime as u64)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.into()
    }
}

impl From<&BucketMeta> for Vec<u8> {
    fn from(b: &BucketMeta) -> Self {
        let mut out = Vec::with_capacity(8 + PTR_SIZE + b.name.len());
        out.extend_from_slice(&b.ctime.to_le_bytes());
        out.extend_from_slice(&b.name.len().to_le_bytes());
        out.extend_from_slice(b.name.as_bytes());
        out
    }
}

impl TryFrom<&[u8]> for BucketMeta {
    type Error = FsError;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() < 8 + PTR_SIZE {
            return Err(FsError::MalformedObject);
        }
        let name_len = usize::from_le_bytes(value[8..8 + PTR_SIZE].try_into().unwrap());
        if value.len() != 8 + PTR_SIZE + name_len {
            return Err(FsError::MalformedObject);
        }
        Ok(BucketMeta {
            ctime: i64::from_le_bytes(value[..8].try_into().unwrap()),
            // SAFETY: this is safe because we only store valid strings in the first place.
            name: unsafe { String::from_utf8_unchecked(value[8 + PTR_SIZE..].to_vec()) },
        })
    }
}
