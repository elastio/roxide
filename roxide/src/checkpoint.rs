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
        path: impl Into<path::PathBuf>,
        checkpoint: impl Into<CheckpointHandle>,
    ) -> Result<Self> {
        let path = path.into();
        let checkpoint = checkpoint.into();
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
    #![allow(clippy::blacklisted_name)]
    use super::*;
    use crate::ops::{
        Compact, CreateCheckpoint, DBOpen, DBOpenReadOnly, Flush, Get, GetLatestSequenceNumber,
        GetLiveFiles, Put, SstFile,
    };
    use crate::test::TempDBPath;
    use crate::{
        db::{db::DB, DBLike},
        DBOptions,
    };
    use glob::glob;
    use walkdir::WalkDir;

    #[test]
    fn create_checkpoint() -> Result<()> {
        let path = TempDBPath::new();
        let db = DB::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        db.put(&cf, "foo", "bar", None)?;

        let _checkpoint = db.create_checkpoint(path.path().join("mycheckpoint"))?;

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

        let checkpoint = db.create_checkpoint(path.path().join("mycheckpoint"))?;

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

    #[ignore] // Sadly this test confirmed my fears that custom CF paths are incompatible with checkpointing
    #[test]
    fn checkpoint_with_custom_cf_paths_is_openable() -> Result<()> {
        // Override the column family data paths with per-CF paths, and see how that effects the
        // structure of the checkpoint.
        let path = TempDBPath::new();
        let foo_path = path.path().join("foo");
        let bar_path = path.path().join("bar");
        let mut options = DBOptions::default();

        options.add_column_family("foo");
        options.set_column_family_path("foo", &foo_path);
        options.add_column_family("bar");
        options.set_column_family_path("bar", &bar_path);

        let db = DB::open(&path, options)?;
        // Write some test data to the database before the checkpoint
        let cf = db.get_cf("foo").unwrap();
        crate::test::fill_db(&db, &cf, 10_000)?;

        let cf = db.get_cf("bar").unwrap();
        crate::test::fill_db(&db, &cf, 10_000)?;

        let checkpoint = db.create_checkpoint(path.path().join("mycheckpoint"))?;

        // Open the checkpoint as if it were a database
        let _check_db = DB::open_readonly(checkpoint.path(), None, false)?;

        // Print out the directory structure for debugging purposes
        for entry in WalkDir::new(path.path()) {
            let entry = entry.unwrap();
            println!(
                "{}: {} ({:?})",
                path.path().display(),
                entry.path().display(),
                entry.file_type()
            );
        }

        Ok(())
    }

    /// It wouldn't make sense to include unused SST files in a checkpoint but this test makes
    /// sure.
    #[test]
    fn checkpoint_contains_only_live_files() -> Result<()> {
        // A checkpoint should be usable as a database by itself, but contain the same data as the
        // database it came from.
        let path = TempDBPath::new();
        let mut options = DBOptions::default();
        options.add_column_family("foo");
        options.add_column_family("bar");
        let db = DB::open(&path, options)?;
        let foo = db.get_cf("foo").unwrap();
        let bar = db.get_cf("bar").unwrap();

        // Write some test data to the database before the checkpoint
        crate::test::fill_db_deterministic(&db, &foo, 42, 20_000)?;
        crate::test::fill_db_deterministic(&db, &bar, 52, 20_000)?;
        db.flush_all(None)?;

        println!("SST files after flush, before compact:");
        dump_live_files(db.get_live_files()?.as_slice());

        // Trigger compaction to organize this data into SSTs
        db.compact_all(&foo, None)?;
        db.compact_all(&bar, None)?;

        println!("SST files after compact:");
        dump_live_files(db.get_live_files()?.as_slice());

        // Now re-write half of the keys we wrote before, using a different random value.  This
        // will mean the next compaction will write some new SSTs
        crate::test::fill_db_deterministic(&db, &foo, 42, 10_000)?;
        crate::test::fill_db_deterministic(&db, &bar, 52, 10_000)?;

        let seq_num = db.get_latest_sequence_number();

        let checkpoint_path = TempDBPath::new();
        // We don't want this directory to already exist
        std::fs::remove_dir_all(checkpoint_path.path()).unwrap();

        // Take a checkpoint of the DB only live files
        //
        // Since we re-wrote some of the keys and didn't explicitly flush, this will force those
        // writes to be flushed from the memtable to new L0 SSTs, which will be part of the
        // checkpoint.
        let checkpoint = db.create_checkpoint(checkpoint_path.path())?;

        println!("SST files after checkpoint:");
        dump_live_files(db.get_live_files()?.as_slice());

        // Force a compaction of the live database, that should compact the L0 and L6 levels
        // together.  This change will NOT be reflected in the checkpoint.
        db.compact_all(&foo, None)?;
        db.compact_all(&bar, None)?;

        println!("SST files in live database after checkpoint and compaction:");
        dump_live_files(db.get_live_files()?.as_slice());

        // Open the checkpoint as if it were a database
        let mut options = DBOptions::default();
        options.add_column_family("foo");
        options.add_column_family("bar");
        let check_db = DB::open_readonly(checkpoint.path(), options, false)?;

        assert_eq!(seq_num, check_db.get_latest_sequence_number());

        // The only SST files in the checkpoint should be the live files
        println!("SST files in checkpoint DB:");
        let sst_files = check_db.get_live_files()?;

        let mut actual_sst_count = 0usize;
        for entry in glob(checkpoint.path().join("*.sst").to_string_lossy().as_ref()).unwrap() {
            let path = entry.unwrap();
            actual_sst_count += 1;

            // Is this file expected?
            match sst_files.iter().find(|sst| sst.file_path == path) {
                Some(file) => {
                    dump_live_files(std::slice::from_ref(file));
                }
                None => {
                    panic!("Checkpoint directory contains SST file '{}' which is not one of the live files",
                        path.display());
                }
            }
        }

        assert_eq!(actual_sst_count,
            sst_files.len(),
            "One or more live SST files were not found on disk.  Here are all expected SST files: {:#?}",
            sst_files);

        Ok(())
    }

    fn dump_live_files(sst_files: &[SstFile]) {
        for file in sst_files {
            println!(
                "SST file {} belongs to CF {} level {}",
                file.file_path.display(),
                file.column_family_name,
                file.level
            );
        }
    }
}
