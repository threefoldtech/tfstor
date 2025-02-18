use std::{
    convert::{TryFrom, TryInto},
    time::SystemTime,
    time::UNIX_EPOCH,
};

use chrono::{SecondsFormat, TimeZone, Utc};
use faster_hex::hex_string;

use super::{BlockID, FsError, BLOCKID_SIZE, PTR_SIZE};

#[derive(Debug)]
pub struct Object {
    object_type: ObjectType,
    size: u64,
    ctime: i64,
    e_tag: BlockID,
    blocks: Vec<BlockID>,
    data: ObjectData,
}

#[derive(Debug)]
#[repr(u8)]
pub enum ObjectType {
    Single,
    Multipart,
    Inline,
}

impl ObjectType {
    fn as_u8(&self) -> u8 {
        match self {
            ObjectType::Single => 0,
            ObjectType::Multipart => 1,
            ObjectType::Inline => 2,
        }
    }
}

#[derive(Debug)]
pub enum ObjectData {
    // The object is stored inline in the metadata.
    Inline {
        data: Vec<u8>,
    },

    // The object is a single part object, and the blocks are stored in the blocks field.
    SinglePart,

    // The object is a multipart object, and the blocks are stored in the blocks field.
    MultiPart {
        // The amount of parts uploaded for this object. In case of a simple put_object, this will be
        // 0. In case of a multipart upload, this wil equal the amount of individual parts. This is
        // required so we can properly construct the formatted hash later.
        parts: usize,
    },
}

impl Object {
    pub fn new(size: u64, e_tag: BlockID, blocks: Vec<BlockID>, object_data: ObjectData) -> Self {
        Self {
            object_type: ObjectType::Single,
            size,
            ctime: Utc::now().timestamp(),
            e_tag,
            blocks,
            data: object_data,
        }
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.into()
    }

    pub fn format_e_tag(&self) -> String {
        if let ObjectData::MultiPart { parts, .. } = &self.data {
            format!("\"{}-{}\"", hex_string(&self.e_tag), parts)
        } else {
            // Handle error case or provide default
            format!("\"{}\"", hex_string(&self.e_tag))
        }
    }

    pub fn touch(&mut self) {
        self.ctime = Utc::now().timestamp();
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn blocks(&self) -> &[BlockID] {
        &self.blocks
    }

    pub fn has_block(&self, block: &BlockID) -> bool {
        self.blocks.contains(block)
    }

    pub fn last_modified(&self) -> SystemTime {
        UNIX_EPOCH + std::time::Duration::from_secs(self.ctime as u64)
    }

    pub fn format_ctime(&self) -> String {
        Utc.timestamp_opt(self.ctime, 0)
            .unwrap()
            .to_rfc3339_opts(SecondsFormat::Secs, true)
    }
}

impl From<&Object> for Vec<u8> {
    fn from(o: &Object) -> Self {
        let mut raw_data =
            Vec::with_capacity(16 + BLOCKID_SIZE + PTR_SIZE * 2 + o.blocks.len() * BLOCKID_SIZE);

        raw_data.extend_from_slice(&o.object_type.as_u8().to_le_bytes());
        raw_data.extend_from_slice(&o.size.to_le_bytes());
        raw_data.extend_from_slice(&o.ctime.to_le_bytes());
        raw_data.extend_from_slice(&o.e_tag);
        raw_data.extend_from_slice(&o.blocks.len().to_le_bytes());
        for block in &o.blocks {
            raw_data.extend_from_slice(block);
        }

        match &o.data {
            ObjectData::SinglePart => {
                raw_data.extend_from_slice(&0u64.to_le_bytes());
            }
            ObjectData::MultiPart { parts } => {
                raw_data.extend_from_slice(&parts.to_le_bytes());
            }
            ObjectData::Inline { data } => {
                raw_data.extend_from_slice(&(data.len() as u64).to_le_bytes());
                raw_data.extend_from_slice(data);
            }
        }

        raw_data
    }
}

impl TryFrom<&[u8]> for Object {
    type Error = FsError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() < 16 + BLOCKID_SIZE + 2 * PTR_SIZE {
            return Err(FsError::MalformedObject);
        }

        // object type: 1 byte
        let mut start: usize = 0;
        let mut end: usize = 1;
        let object_type = u8::from_le_bytes(value[start..end].try_into().unwrap());
        let object_type = match object_type {
            0 => ObjectType::Single,
            1 => ObjectType::Multipart,
            2 => ObjectType::Inline,
            _ => return Err(FsError::MalformedObject),
        };

        // size: 8 bytes
        start += 1;
        end += 8;
        let size = u64::from_le_bytes(value[start..end].try_into().unwrap());

        // ctime: 8bytes
        start += 8;
        end += 8;
        let ctime = i64::from_le_bytes(value[start..end].try_into().unwrap());

        // etag: BLOCKID_SIZE bytes
        start += 8;
        end += BLOCKID_SIZE;
        let e_tag = value[start..end].try_into().unwrap();

        // block_len : PTR_SIZE bytes
        start += BLOCKID_SIZE;
        end += PTR_SIZE;
        let block_len = usize::from_le_bytes(value[start..end].try_into().unwrap());

        //if value.len() != 16 + 2 * PTR_SIZE + BLOCKID_SIZE + block_len * BLOCKID_SIZE {
        //    return Err(FsError::MalformedObject);
        //}

        let mut blocks = Vec::with_capacity(block_len);

        // blocks: BLOCKID_SIZE * block_len bytes
        start += PTR_SIZE;
        end += BLOCKID_SIZE * block_len;
        for chunk in value[start..end].chunks_exact(BLOCKID_SIZE) {
            blocks.push(chunk.try_into().unwrap());
        }

        let data = match object_type {
            ObjectType::Single => ObjectData::SinglePart,
            ObjectType::Multipart => {
                // parts: PTR_SIZE bytes
                start += BLOCKID_SIZE * block_len;
                end += PTR_SIZE;
                let parts = usize::from_le_bytes(value[start..end].try_into().unwrap());
                ObjectData::MultiPart { parts }
            }
            ObjectType::Inline => {
                // data_len: PTR_SIZE bytes
                start += BLOCKID_SIZE * block_len;
                end += PTR_SIZE;
                let data_len = u64::from_le_bytes(value[start..end].try_into().unwrap());

                // data: data_len bytes
                start += PTR_SIZE;
                end += data_len as usize;
                let data = value[start..end].to_vec();
                ObjectData::Inline { data }
            }
        };
        Ok(Self {
            object_type,
            size,
            ctime,
            e_tag,
            blocks,
            data,
        })
    }
}
