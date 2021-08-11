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
pub trait CreateCheckpoint: RocksOpBase {
    /// Create a point-in-time snapshot of the database in `path`
    ///
    /// If `path` is a location on the same filesystem as the live database files, the checkpoint
    /// will use hard links instead of file copies for all of the SST files so it should be very
    /// fast.
    ///
    /// `log_size_for_flush` controls whether or not memtables will be flushed before the
    /// checkpoint:
    /// * `0` - always flush all memtables to disk before the checkpoint
    /// * `u64::MAX` - never flush no matter what
    /// * anything else - flush if the size of the active WAL log is greater than this number of
    /// bytes.
    ///
    /// If flushing doesn't occur, the checkpoint will likely include a WAL file, which will need
    /// to be used to recover the not-yet-flushed operations which were still in memtables at the
    /// time the checkpoint was taken.
    fn create_checkpoint(
        &self,
        path: impl Into<path::PathBuf>,
        log_size_for_flush: u64,
    ) -> Result<checkpoint::Checkpoint>;
}

impl CreateCheckpoint for Db {
    fn create_checkpoint(
        &self,
        path: impl Into<path::PathBuf>,
        log_size_for_flush: u64,
    ) -> Result<checkpoint::Checkpoint> {
        op_metrics::instrument_db_op(
            self,
            op_metrics::DatabaseOperation::Checkpoint,
            move || unsafe {
                let checkpoint_ptr = ffi_try!(ffi::rocksdb_checkpoint_object_create(
                    self.rocks_ptr().as_ptr(),
                ))?;

                if let Some(checkpoint_ptr) = ptr::NonNull::new(checkpoint_ptr) {
                    checkpoint::Checkpoint::new(self, path, log_size_for_flush, checkpoint_ptr)
                } else {
                    Err(Error::other_error("checkpoint object creation failed"))
                }
            },
        )
    }
}

impl CreateCheckpoint for TransactionDb {
    fn create_checkpoint(
        &self,
        path: impl Into<path::PathBuf>,
        log_size_for_flush: u64,
    ) -> Result<checkpoint::Checkpoint> {
        op_metrics::instrument_db_op(
            self,
            op_metrics::DatabaseOperation::Checkpoint,
            move || unsafe {
                let checkpoint_ptr = ffi_try!(
                    ffi::rocksdb_transactiondb_checkpoint_object_create(self.rocks_ptr().as_ptr(),)
                )?;

                if let Some(checkpoint_ptr) = ptr::NonNull::new(checkpoint_ptr) {
                    checkpoint::Checkpoint::new(self, path, log_size_for_flush, checkpoint_ptr)
                } else {
                    Err(Error::other_error("checkpoint object creation failed"))
                }
            },
        )
    }
}

impl CreateCheckpoint for OptimisticTransactionDb {
    fn create_checkpoint(
        &self,
        path: impl Into<path::PathBuf>,
        log_size_for_flush: u64,
    ) -> Result<checkpoint::Checkpoint> {
        // The optimistic transaction DB API is slightly different than either of the others.
        // See the comment in `put.rs` for the details
        op_metrics::instrument_db_op(
            self,
            op_metrics::DatabaseOperation::Checkpoint,
            move || unsafe {
                Self::with_base_db(self.handle(), |base_db| {
                    let checkpoint_ptr = ffi_try!(ffi::rocksdb_checkpoint_object_create(base_db,))?;

                    if let Some(checkpoint_ptr) = ptr::NonNull::new(checkpoint_ptr) {
                        checkpoint::Checkpoint::new(self, path, log_size_for_flush, checkpoint_ptr)
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
    use crate::db::DbLike;
    use crate::ops::{DbOpen, Put};
    use crate::test::TempDbPath;

    #[test]
    fn db_checkpoint() -> Result<()> {
        let path = TempDbPath::new();
        let db = Db::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        db.put(&cf, "foo", "bar", None)?;

        let _checkpoint = db.create_checkpoint(path.path().join("mycheckpoint"), 0)?;

        Ok(())
    }

    #[test]
    fn txdb_checkpoint() -> Result<()> {
        let path = TempDbPath::new();
        let db = TransactionDb::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        db.put(&cf, "foo", "bar", None)?;

        let _checkpoint = db.create_checkpoint(path.path().join("mycheckpoint"), 0)?;

        Ok(())
    }

    #[test]
    fn opt_txdb_checkpoint() -> Result<()> {
        let path = TempDbPath::new();
        let db = OptimisticTransactionDb::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        db.put(&cf, "foo", "bar", None)?;

        let _checkpoint = db.create_checkpoint(path.path().join("mycheckpoint"), 0)?;

        Ok(())
    }

    /// An unexpected result that this test confirms is that checkpoints of a database get a new
    /// ID.
    /// WTF??
    #[test]
    fn checkpoint_id_changes() -> Result<()> {
        let path = TempDbPath::new();
        let db = Db::open(&path, None)?;
        let id = db.get_db_id().unwrap();
        let cf = db.get_cf("default").unwrap();

        db.put(&cf, "foo", "bar", None)?;

        let checkpoint = db.create_checkpoint(path.path().join("mycheckpoint"), 0)?;

        let checkpoint_db = Db::open(checkpoint.path(), None)?;
        assert_ne!(id, checkpoint_db.get_db_id().unwrap());

        Ok(())
    }
}
