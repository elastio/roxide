//! The `Checkpoint` operation reads a single key from the database
use super::op_metrics;
use super::*;
use crate::checkpoint;
use crate::db::{db::*, opt_txdb::*, txdb::*};
use crate::ffi;
use crate::handle::RocksObject;
use crate::{Error, Result};
use std::path;
use std::ptr;

/// Trait implemented by database types which support creating point-in-time snapshots of the
/// database files on disk.
pub trait Checkpoint: RocksOpBase {
    /// Checkpoints the value for `key` in `cf`, returning a Rocks-allocated buffer containing the value
    /// if found, or `None` if not found.
    fn checkpoint(&self, path: impl AsRef<path::Path>) -> Result<checkpoint::Checkpoint>;
}

impl Checkpoint for DB {
    fn checkpoint(&self, path: impl AsRef<path::Path>) -> Result<checkpoint::Checkpoint> {
        op_metrics::instrument_db_op(
            self,
            op_metrics::DatabaseOperation::Checkpoint,
            move || unsafe {
                let checkpoint_ptr = ffi_try!(ffi::rocksdb_checkpoint_object_create(
                    self.rocks_ptr().as_ptr(),
                ))?;

                if let Some(checkpoint_ptr) = ptr::NonNull::new(checkpoint_ptr) {
                    checkpoint::Checkpoint::new(self, path, checkpoint_ptr)
                } else {
                    Err(Error::other_error("checkpoint object creation failed"))
                }
            },
        )
    }
}

impl Checkpoint for TransactionDB {
    fn checkpoint(&self, path: impl AsRef<path::Path>) -> Result<checkpoint::Checkpoint> {
        op_metrics::instrument_db_op(
            self,
            op_metrics::DatabaseOperation::Checkpoint,
            move || unsafe {
                let checkpoint_ptr = ffi_try!(
                    ffi::rocksdb_transactiondb_checkpoint_object_create(self.rocks_ptr().as_ptr(),)
                )?;

                if let Some(checkpoint_ptr) = ptr::NonNull::new(checkpoint_ptr) {
                    checkpoint::Checkpoint::new(self, path, checkpoint_ptr)
                } else {
                    Err(Error::other_error("checkpoint object creation failed"))
                }
            },
        )
    }
}

impl Checkpoint for OptimisticTransactionDB {
    fn checkpoint(&self, path: impl AsRef<path::Path>) -> Result<checkpoint::Checkpoint> {
        // The optimistic transaction DB API is slightly different than either of the others.
        // See the comment in `put.rs` for the details
        op_metrics::instrument_db_op(
            self,
            op_metrics::DatabaseOperation::Checkpoint,
            move || unsafe {
                Self::with_base_db(self.handle(), |base_db| {
                    let checkpoint_ptr = ffi_try!(ffi::rocksdb_checkpoint_object_create(base_db,))?;

                    if let Some(checkpoint_ptr) = ptr::NonNull::new(checkpoint_ptr) {
                        checkpoint::Checkpoint::new(self, path, checkpoint_ptr)
                    } else {
                        Err(Error::other_error("checkpoint object creation failed"))
                    }
                })
            },
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::DBLike;
    use crate::ops::{DBOpen, Put};
    use crate::test::TempDBPath;

    #[test]
    fn db_checkpoint() -> Result<()> {
        let path = TempDBPath::new();
        let db = DB::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        db.put(&cf, "foo", "bar", None)?;

        let _checkpoint = db.checkpoint(path.path().join("mycheckpoint"))?;

        Ok(())
    }

    #[test]
    fn txdb_checkpoint() -> Result<()> {
        let path = TempDBPath::new();
        let db = TransactionDB::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        db.put(&cf, "foo", "bar", None)?;

        let _checkpoint = db.checkpoint(path.path().join("mycheckpoint"))?;

        Ok(())
    }

    #[test]
    fn opt_txdb_checkpoint() -> Result<()> {
        let path = TempDBPath::new();
        let db = OptimisticTransactionDB::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        db.put(&cf, "foo", "bar", None)?;

        let _checkpoint = db.checkpoint(path.path().join("mycheckpoint"))?;

        Ok(())
    }
}
