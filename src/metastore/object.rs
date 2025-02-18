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
    data: ObjectData,
}

#[derive(Debug)]
pub enum ObjectData {
    // The object is stored inline in the metadata.
    Inline {
        data: Vec<u8>,
    },

    // The object is a single part object, and the blocks are stored in the blocks field.
    SinglePart {
        blocks: Vec<BlockID>,
    },

    // The object is a multipart object, and the blocks are stored in the blocks field.
    MultiPart {
        blocks: Vec<BlockID>,
        // The amount of parts uploaded for this object. In case of a simple put_object, this will be
        // 0. In case of a multipart upload, this wil equal the amount of individual parts. This is
        // required so we can properly construct the formatted hash later.
        parts: usize,
    },
}

#[derive(Debug, PartialEq)]
#[repr(u8)]
pub enum ObjectType {
    Single = 0,
    Multipart = 1,
    Inline = 2,
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

impl Object {
    pub fn new(size: u64, e_tag: BlockID, object_data: ObjectData) -> Self {
        let object_type = match &object_data {
            ObjectData::SinglePart { .. } => ObjectType::Single,
            ObjectData::MultiPart { .. } => ObjectType::Multipart,
            ObjectData::Inline { .. } => ObjectType::Inline,
        };
        Self {
            object_type,
            size,
            ctime: Utc::now().timestamp(),
            e_tag,
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
        match &self.data {
            ObjectData::SinglePart { blocks } => blocks,
            ObjectData::MultiPart { blocks, .. } => blocks,
            ObjectData::Inline { .. } => &[],
        }
    }

    pub fn has_block(&self, block: &BlockID) -> bool {
        match &self.data {
            ObjectData::SinglePart { blocks } => blocks.contains(block),
            ObjectData::MultiPart { blocks, .. } => blocks.contains(block),
            ObjectData::Inline { .. } => false,
        }
    }

    pub fn last_modified(&self) -> SystemTime {
        UNIX_EPOCH + std::time::Duration::from_secs(self.ctime as u64)
    }

    pub fn format_ctime(&self) -> String {
        Utc.timestamp_opt(self.ctime, 0)
            .unwrap()
            .to_rfc3339_opts(SecondsFormat::Secs, true)
    }

    fn num_bytes(&self) -> usize {
        let mandatory_fields_size = 17 + BLOCKID_SIZE;
        match &self.data {
            ObjectData::SinglePart { blocks } => {
                mandatory_fields_size + (blocks.len() * BLOCKID_SIZE)
            }
            ObjectData::MultiPart { blocks, .. } => {
                mandatory_fields_size + PTR_SIZE + (blocks.len() * BLOCKID_SIZE) + PTR_SIZE
            }
            ObjectData::Inline { data } => mandatory_fields_size + PTR_SIZE + data.len(),
        }
    }
}

impl From<&Object> for Vec<u8> {
    fn from(o: &Object) -> Self {
        let mut raw_data = Vec::with_capacity(o.num_bytes());

        // Write header fields
        raw_data.extend_from_slice(&o.object_type.as_u8().to_le_bytes());
        raw_data.extend_from_slice(&o.size.to_le_bytes());
        raw_data.extend_from_slice(&o.ctime.to_le_bytes());
        raw_data.extend_from_slice(&o.e_tag);

        // Write variant-specific data
        match &o.data {
            ObjectData::SinglePart { blocks } | ObjectData::MultiPart { blocks, .. } => {
                // Write blocks for both Single and MultiPart
                raw_data.extend_from_slice(&blocks.len().to_le_bytes());
                blocks
                    .iter()
                    .for_each(|block| raw_data.extend_from_slice(block));

                // Write parts count for MultiPart only
                if let ObjectData::MultiPart { parts, .. } = &o.data {
                    raw_data.extend_from_slice(&parts.to_le_bytes());
                }
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
        let mut pos = 0;

        let object_type = u8::from_le_bytes(value[pos..pos + 1].try_into().unwrap());
        let object_type = match object_type {
            0 => ObjectType::Single,
            1 => ObjectType::Multipart,
            2 => ObjectType::Inline,
            _ => return Err(FsError::MalformedObject),
        };
        pos += 1;

        // size: 8 bytes
        let size = u64::from_le_bytes(value[pos..pos + 8].try_into().unwrap());
        pos += 8;

        // ctime: 8bytes
        let ctime = i64::from_le_bytes(value[pos..pos + 8].try_into().unwrap());
        pos += 8;

        // etag: BLOCKID_SIZE bytes
        let e_tag = value[pos..pos + BLOCKID_SIZE].try_into().unwrap();
        pos += BLOCKID_SIZE;

        let data = match object_type {
            ObjectType::Single | ObjectType::Multipart => {
                // block_len : PTR_SIZE bytes
                let block_len =
                    usize::from_le_bytes(value[pos..pos + PTR_SIZE].try_into().unwrap());
                pos += PTR_SIZE;

                // check the expected length
                let mut expected_len = pos + block_len * BLOCKID_SIZE;
                if object_type == ObjectType::Multipart {
                    expected_len += PTR_SIZE;
                }
                if value.len() != expected_len {
                    return Err(FsError::MalformedObject);
                }

                let mut blocks = Vec::with_capacity(block_len);

                // blocks: BLOCKID_SIZE * block_len bytes
                for chunk in value[pos..pos + (BLOCKID_SIZE * block_len)].chunks_exact(BLOCKID_SIZE)
                {
                    blocks.push(chunk.try_into().unwrap());
                }
                pos += BLOCKID_SIZE * block_len;

                if object_type == ObjectType::Single {
                    ObjectData::SinglePart { blocks }
                } else {
                    let parts =
                        usize::from_le_bytes(value[pos..pos + PTR_SIZE].try_into().unwrap());
                    ObjectData::MultiPart { blocks, parts }
                }
            }
            ObjectType::Inline => {
                // data_len: PTR_SIZE bytes
                let data_len = u64::from_le_bytes(value[pos..pos + PTR_SIZE].try_into().unwrap());
                pos += PTR_SIZE;

                // check the expected length
                let expected_len = pos + data_len as usize;
                if value.len() != expected_len {
                    return Err(FsError::MalformedObject);
                }

                // data: data_len bytes
                let data = value[pos..pos + data_len as usize].to_vec();
                ObjectData::Inline { data }
            }
        };
        Ok(Self {
            object_type,
            size,
            ctime,
            e_tag,
            data,
        })
    }
}
