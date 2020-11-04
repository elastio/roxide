//! A checkpoint in RocksDB is a point in time snapshot of the state of the database files, in a
//! directory on disk.  If the directory is on the same filesystem as the database itself, these
//! files are hard links to the live database files, meaning it's very fast to create a checkpoint
//! and doesn't take up any extra space.
//!
//! We use checkpoints extensively because unlike snapshots they let us operate on individual
//! database files when we sync the database to S3.
use crate::db;
use crate::ffi;
use crate::ffi_util;
use crate::handle;
use crate::rocks_class;
use crate::Result;
use std::path;
use std::ptr;

rocks_class!(CheckpointHandle, ffi::rocksdb_checkpoint_t, ffi::rocksdb_checkpoint_object_destroy, @send);

pub struct Checkpoint {
    inner: CheckpointHandle,

    path: path::PathBuf,

    /// The handle to the database this checkpoint is opened on.  Since struct members are dropped
    /// in order of declaration, this ensures the checkpoint handle `inner` is dropped first, so
    /// that even if the last remaining handle to the database is this one, the checkpoint still
    /// gets dropped first
    _db: handle::AnonymousHandle,
}

impl Checkpoint {
    /// Creates a new `Checkpoint` on disk, using a checkpoint object previously created by one of
    /// the database implementations.
    ///
    /// Note that the caller of this function (one of the database impls) has used a Rocks FFI call
    /// to create a new checkpoint object.  That does not actually create a checkpoint on disk, in
    /// much the same way that allocating a buffer for a read is separate from the actual read
    /// operation.
    ///
    /// What `new` does here is the actual creation of the snapshot on disk.
    pub(crate) fn new<DB: db::DBLike>(
        db: &DB,
        path: impl AsRef<path::Path>,
        checkpoint: impl Into<CheckpointHandle>,
    ) -> Result<Self> {
        let checkpoint = checkpoint.into();
        let path = path.as_ref().to_owned();
        let cpath = ffi_util::path_to_cstring(&path)?;

        unsafe {
            ffi_try!(ffi::rocksdb_checkpoint_create(
                checkpoint.as_ptr(),
                cpath.as_ptr(),
                0, // log_size_for_flush; 0 means always flush WAL before checkpointing
            ))?;
        }

        Ok(Checkpoint {
            inner: checkpoint,
            path,
            _db: db.db_handle(),
        })
    }

    pub fn path(&self) -> &path::Path {
        &self.path
    }
}

impl handle::RocksObject<ffi::rocksdb_checkpoint_t> for Checkpoint {
    fn rocks_ptr(&self) -> ptr::NonNull<ffi::rocksdb_checkpoint_t> {
        self.inner.rocks_ptr()
    }
}

/// Checkpoint objects are read-only and immutable, so they're safe to send and sync
unsafe impl Send for Checkpoint {}
unsafe impl Sync for Checkpoint {}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::{db::DB, DBLike};
    use crate::ops::{Checkpoint, DBOpen, DBOpenReadOnly, Get, Put};
    use crate::test::TempDBPath;

    #[test]
    fn create_checkpoint() -> Result<()> {
        let path = TempDBPath::new();
        let db = DB::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        db.put(&cf, "foo", "bar", None)?;

        let _checkpoint = db.checkpoint(path.path().join("mycheckpoint"))?;

        Ok(())
    }

    #[test]
    fn checkpoint_is_openable() -> Result<()> {
        // A checkpoint should be usable as a database by itself, but contain the same data as the
        // database it came from.
        let path = TempDBPath::new();
        let db = DB::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        // Write some test data to the database before the checkpoint
        db.put(&cf, "foo", "bar", None)?;

        let checkpoint = db.checkpoint(path.path().join("mycheckpoint"))?;

        // Write more data after the checkpoint
        db.put(&cf, "foo", "baz", None)?;

        // Open the checkpoint as if it were a database
        // `false` means the open should not fail if there is a WAL.  Even though `checkpoint`
        // flushes the log, it seems in practice a log file still exists in the checkpoint dir
        let check_db = DB::open_readonly(checkpoint.path(), None, false)?;

        // The value before the checkpoint should be returned
        assert_eq!(
            "bar",
            check_db.get(&cf, "foo", None)?.unwrap().to_string_lossy()
        );

        Ok(())
    }
}
