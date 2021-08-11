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
    /// Checkpoints the value for `key` in `cf`, returning a Rocks-allocated buffer containing the value
    /// if found, or `None` if not found.
    fn create_checkpoint(
        &self,
        path: impl Into<path::PathBuf>,
        log_size_for_flush: Option<u64>,
    ) -> Result<checkpoint::Checkpoint>;
}

impl CreateCheckpoint for Db {
    fn create_checkpoint(
        &self,
        path: impl Into<path::PathBuf>,
        log_size_for_flush: Option<u64>,
    ) -> Result<checkpoint::Checkpoint> {
        op_metrics::instrument_db_op(
            self,
            op_metrics::DatabaseOperation::Checkpoint,
            move || unsafe {
                let checkpoint_ptr = ffi_try!(ffi::rocksdb_checkpoint_object_create(
                    self.rocks_ptr().as_ptr(),
                ))?;

                if let Some(checkpoint_ptr) = ptr::NonNull::new(checkpoint_ptr) {
                    checkpoint::Checkpoint::new(
                        self,
                        path,
                        checkpoint_ptr,
                        log_size_for_flush.unwrap_or_default(),
                    )
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
        log_size_for_flush: Option<u64>,
    ) -> Result<checkpoint::Checkpoint> {
        // Let's inform the guy, who will try to switch to TransactionDb next time, that there is one more
        // problem with that: https://github.com/elastio/elastio/issues/2829
        if log_size_for_flush.is_some() {
            panic!("log_size_for_flush is not supported (has no effect) by TransactionDb. This may have significant negative impact on checkpoint/sync performance")
        }

        op_metrics::instrument_db_op(
            self,
            op_metrics::DatabaseOperation::Checkpoint,
            move || unsafe {
                let checkpoint_ptr = ffi_try!(
                    ffi::rocksdb_transactiondb_checkpoint_object_create(self.rocks_ptr().as_ptr(),)
                )?;

                if let Some(checkpoint_ptr) = ptr::NonNull::new(checkpoint_ptr) {
                    checkpoint::Checkpoint::new(
                        self,
                        path,
                        checkpoint_ptr,
                        // This works for Db and OptimisticTransactionDb but seems to be ignored for TransactionDb on the Rocks side.
                        // Fortunately we haven't switched to TransactionDb and it is not our primary implementation.
                        log_size_for_flush.unwrap_or_default(),
                    )
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
        log_size_for_flush: Option<u64>,
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
                        checkpoint::Checkpoint::new(
                            self,
                            path,
                            checkpoint_ptr,
                            log_size_for_flush.unwrap_or_default(),
                        )
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
    use std::ffi::OsStr;

    #[test]
    fn db_checkpoint() -> Result<()> {
        let path = TempDbPath::new();
        let db = Db::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        db.put(&cf, "foo", "bar", None)?;

        let _checkpoint = db.create_checkpoint(path.path().join("mycheckpoint"), None)?;

        Ok(())
    }

    #[test]
    fn db_log_size_for_flush_test() -> Result<()> {
        let path = TempDbPath::new();
        let db = Db::open(&path, None)?;
        log_size_for_flush_test(db, path)?;

        Ok(())
    }

    #[test]
    fn optxdb_log_size_for_flush_test() -> Result<()> {
        let path = TempDbPath::new();
        let db = OptimisticTransactionDb::open(&path, None)?;
        log_size_for_flush_test(db, path)?;

        Ok(())
    }

    #[test]
    #[should_panic]
    fn txdb_log_size_for_flush_not_supported_panics() {
        let path = TempDbPath::new();
        let db = TransactionDb::open(&path, None).unwrap();
        log_size_for_flush_test(db, path).unwrap();
    }

    fn log_size_for_flush_test<TDB: DbLike>(db: TDB, path: TempDbPath) -> Result<()> {
        let cf = db.get_cf("default").unwrap();

        db.put(&cf, "foo", "bar", None)?;

        let _checkpoint = db.create_checkpoint(path.path().join("chkpt1"), Some(1024 * 1024))?;

        // Since log_size_for_flush was set to 1MB, RocksDB shouldn't flush the memtable and just
        // include log file into the checkpoint.
        // Expected result: No SST files on disc
        let mut rd = std::fs::read_dir(path.path())?;
        assert!(!rd.any(|x| x.unwrap().path().extension().and_then(OsStr::to_str) == Some("sst")));

        // Now let's create another checkpoint with 'None' as log_size_for_flush which should be
        // defaulted to 0 causing memtable flush resulting into at least one SST file on disk
        let _checkpoint = db.create_checkpoint(path.path().join("chkpt2"), None)?;
        let mut rd = std::fs::read_dir(path.path())?;
        assert!(rd.any(|x| x.unwrap().path().extension().and_then(OsStr::to_str) == Some("sst")));

        Ok(())
    }

    #[test]
    fn txdb_checkpoint() -> Result<()> {
        let path = TempDbPath::new();
        let db = TransactionDb::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        db.put(&cf, "foo", "bar", None)?;

        let _checkpoint = db.create_checkpoint(path.path().join("mycheckpoint"), None)?;

        Ok(())
    }

    #[test]
    fn opt_txdb_checkpoint() -> Result<()> {
        let path = TempDbPath::new();
        let db = OptimisticTransactionDb::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        db.put(&cf, "foo", "bar", None)?;

        let _checkpoint = db.create_checkpoint(path.path().join("mycheckpoint"), None)?;

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

        let checkpoint = db.create_checkpoint(path.path().join("mycheckpoint"), None)?;

        let checkpoint_db = Db::open(checkpoint.path(), None)?;
        assert_ne!(id, checkpoint_db.get_db_id().unwrap());

        Ok(())
    }
}
