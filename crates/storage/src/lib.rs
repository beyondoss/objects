pub mod error;
pub mod types;

mod bucket;
mod gc;
mod multipart;
mod read;
pub(crate) mod write;
mod xattr;

use std::path::PathBuf;

pub use error::StorageError;
pub use types::{
    AccessLevel, BucketMeta, CompletedPart, MultipartInfo, ObjectInfo, ObjectMeta, PartInfo,
    WriteCondition,
};

pub type Result<T> = std::result::Result<T, StorageError>;

#[derive(Clone, Debug)]
pub struct Storage {
    pub(crate) data_dir: PathBuf,
}

impl Storage {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
        }
    }
}
