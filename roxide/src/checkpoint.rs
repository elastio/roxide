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
use crate::labels::DatabaseLabels;
use crate::metrics;
use crate::rocks_class;
use crate::status;
use crate::{Cache, DbOptions, Result, SstFile};
use cheburashka::{logging::prelude::*, metrics::*};
use lazy_static::lazy_static;
use maplit::hashmap;
use std::path;
use std::ptr;

rocks_class!(CheckpointHandle, ffi::rocksdb_checkpoint_t, ffi::rocksdb_checkpoint_object_destroy, @send);

cpp! {{
    #include "src/checkpoint.h"
    #include "src/checkpoint.cpp"
}}

// Declare the C helper code that supports taking a checkpoint
extern "C" {
    fn create_checkpoint(
        checkpoint: *const ffi::rocksdb_checkpoint_t,
        path: *const libc::c_char,
        log_size_for_flush: u64,
        sequence_number: *mut u64,
    ) -> status::CppStatus;
}

lazy_static! {
    static ref CHECKPOINTS_TOTAL: metrics::DatabaseIntCounter =
        DatabaseLabels::register_int_counter(
            "rocksdb_db_checkpoints_total",
            "The number of checkpoints performed",
        )
        .unwrap();
    static ref CHECKPOINT_SIZE_BYTES: metrics::DatabaseIntGauge =
        DatabaseLabels::register_int_gauge(
            "rocksdb_db_checkpoint_size_bytes",
            "The size in bytes of the most recent checkpoint",
        )
        .unwrap();
    static ref CHECKPOINT_FILE_COUNT: metrics::DatabaseIntGauge =
        DatabaseLabels::register_int_gauge(
            "rocksdb_db_checkpoint_file_count",
            "The number of files in the most recent checkpoint",
        )
        .unwrap();
}

pub struct Checkpoint {
    inner: CheckpointHandle,

    path: path::PathBuf,

    /// The sequence number of the database at the moment the checkpoint was created
    sequence_number: u64,

    /// The default block cache used by the database.
    ///
    /// This is used if the checkpointed copy of the database needs to be opened to get the SST
    /// file info, to avoid having to create another block cache unnecessarily
    block_cache: Cache,

    /// The options with which the source database was opened.  These options will be needed in
    /// order to open the checkpoint database successfully, particularly if there are any custom
    /// merge operators defined.
    db_options: DbOptions,

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
    #[instrument(
        skip(db, path, checkpoint),
        fields(db_path = %db.path().display(), db_id = %db.db_id(), log_size_for_flush),
        err)
    ]
    pub(crate) fn new<DB: db::DbLike>(
        db: &DB,
        path: impl Into<path::PathBuf>,
        log_size_for_flush: u64,
        checkpoint: impl Into<CheckpointHandle>,
    ) -> Result<Self> {
        let path = path.into();
        let checkpoint = checkpoint.into();
        let cpath = ffi_util::path_to_cstring(&path)?;
        let mut sequence_number = 0u64;

        unsafe {
            let checkpoint_ptr = checkpoint.as_ptr();
            let cpath_ptr = cpath.as_ptr();
            let sequence_number_ptr: *mut u64 = &mut sequence_number;

            create_checkpoint(
                checkpoint_ptr,
                cpath_ptr,
                log_size_for_flush,
                sequence_number_ptr,
            )
            .into_result()?;
        }

        let checkpoint = Checkpoint {
            inner: checkpoint,
            path,
            sequence_number,
            block_cache: db.block_cache().clone(),
            db_options: db.db_options().clone(),
            _db: db.db_handle(),
        };

        // Capture some metrics about the checkpoint.  If this fails it should not fail checkpoint
        // creation
        let labels = DatabaseLabels::from(db);
        if let Err(e) = checkpoint.report_metrics(labels) {
            warn!(
                err = log_error(&e),
                path = %checkpoint.path().display(),
                "error updating checkpoint metrics; checkpoint operation will proceed"
            );
        }

        Ok(checkpoint)
    }

    pub fn path(&self) -> &path::Path {
        &self.path
    }

    /// The sequence number at which this checkpoint was taken.
    ///
    /// It is guaranteed that all operations made prior to this sequence number are included in the
    /// checkpoint.  Any operation made after this sequence number is not reflected in the
    /// checkpoint.
    pub fn sequence_number(&self) -> u64 {
        self.sequence_number
    }

    /// Open the checkpoint's database read-only and get all of the SST files that make up that
    /// checkpoint.
    ///
    /// Note that this is, conceptually, equivalent to what one can achieve by using `roxide` to
    /// open the checkpoint as a regular database and then call
    /// [`crate::GetLiveFiles::get_live_files`] on it.  However this is much better optimized
    /// because it knows the checkpoint database is not being opened to be queried or written to
    /// but just to quickly get metadata and then close.  Thus it will disable extra integrity
    /// checks and will re-use the same cache as the live database this checkpoint came from,
    /// rather than potentially allocating another cache just for this short-lived operation.
    ///
    /// This also does not report any database-level metrics for the checkpoint, which is a big
    /// cause of <https://github.com/elastio/elastio/issues/2457>
    #[instrument(skip(self), fields(path = %self.path.display()))]
    pub fn get_sst_files(&self) -> Result<Vec<SstFile>> {
        // Using the source database's options as a starting point, tune the config a bit to make
        // opening the checkpoint database as fast as it can be.  We are only opening it to call
        // `GetLiveFilesMetaData`, we won't be querying anything, we won't be writing to the
        // database at all.
        //
        // Some of the tips in https://github.com/facebook/rocksdb/wiki/Speed-Up-DB-Open are used
        // here
        let mut db_options = self.db_options.clone();
        db_options.add_db_options(hashmap! {
            // Don't bother updating stats on files because we don't care
            "skip_stats_update_on_db_open" => "false",

            // Don't validate SST file sizes, we know they're fine
            "skip_checking_sst_file_sizes_on_db_open" => "false",

            // Don't bother opening all files and reading their metadata into memory
            // Open question: what is the lowest practical value that will work, given that we're
            // not going to read anything out of this database?
            "max_open_files" => "10"
        });

        let mut block_table_options = db_options.get_default_cf_block_table_options();
        block_table_options.extend(
            hashmap! {
                // Do not attempt to cache index or filter blocks at all
                "cache_index_and_filter_blocks" => "false",
                "cache_index_and_filter_blocks_with_high_priority" => "true",

                // Do not pin any index or filter blocks
                // This probably doesn't do anything if caching index and filter block is disabled
                // but it doesn't hurt either.
                "pin_top_level_index_and_filter" => "true",
                "pin_l0_filter_and_index_blocks_in_cache" => "true",
            }
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string())),
        );
        db_options.set_default_cf_block_table_options(block_table_options);

        // Use the same block cache as the source database.  This getting live files shouldn't
        // actually put anything in the cache at all, but we don't want Rocks to allocate a
        // separate default block cache just for this database because it's useless.
        db_options.force_block_cache(self.block_cache.clone());

        // Open the database using the read-only database type, which will skip initialization of
        // some expensive locking structures because it knows there will be no writes
        debug!("Opening checkpoint database to get live files");
        let (db_ptr, _cache, _db_options, cfs) = crate::ops::ffi_open_helper(
            self.path(),
            db_options,
            |options,
             path,
             num_column_families,
             column_family_names,
             column_family_options,
             column_family_handles| {
                unsafe {
                    ffi_try!(ffi::rocksdb_open_for_read_only_column_families(
                        options,
                        path,
                        num_column_families,
                        column_family_names,
                        column_family_options,
                        column_family_handles,
                        0, // fail_if_log_file_exists = false
                    ))
                }
            },
        )?;

        // Immediately wrap the DB in a handle so it won't leak on error
        let db_handle = db::DbHandle::new(db_ptr);
        let db_c_ptr = db_handle.as_ptr();

        // Drop the column families now we don't need them, and they need to drop before the
        // database handle itself
        drop(cfs);

        let live_files = unsafe {
            // Get the C++ `DB` pointer from the C wrapper
            let db_cpp_ptr = cpp!([db_c_ptr as "rocksdb_t*"] -> *mut libc::c_void as "rocksdb::DB*" {
                return cast_to_db(db_c_ptr);
            });

            // Query the live file metadata
            crate::ops::get_live_files_impl(db_cpp_ptr)
        }?;

        debug!(
            live_files_len = live_files.len(),
            "Got live files from checkpoint"
        );

        Ok(live_files)
    }

    /// Update the metrics with the information about this checkpoint
    fn report_metrics(&self, labels: DatabaseLabels) -> Result<()> {
        CHECKPOINTS_TOTAL.apply_labels(&labels).unwrap().inc();
        let mut num_files = 0usize;
        let mut num_bytes = 0u64;

        for entry in std::fs::read_dir(self.path())? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                num_files += 1;

                let metadata = std::fs::metadata(path)?;
                num_bytes += metadata.len();
            }
        }

        CHECKPOINT_FILE_COUNT
            .apply_labels(&labels)
            .unwrap()
            .set(num_files as i64);

        CHECKPOINT_SIZE_BYTES
            .apply_labels(&labels)
            .unwrap()
            .set(num_bytes as i64);

        Ok(())
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
        Compact, CreateCheckpoint, DbOpen, DbOpenReadOnly, Flush, Get, GetLatestSequenceNumber,
        GetLiveFiles, Put, SstFile,
    };
    use crate::test::TempDbPath;
    use crate::{
        db::{db::Db, DbLike},
        DbOptions,
    };
    use glob::glob;
    use walkdir::WalkDir;

    #[test]
    fn create_checkpoint() -> Result<()> {
        let path = TempDbPath::new();
        let db = Db::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        db.put(&cf, "foo", "bar", None)?;

        let _checkpoint = db.create_checkpoint(path.path().join("mycheckpoint"), 0)?;

        Ok(())
    }

    #[test]
    fn checkpoint_is_openable() -> Result<()> {
        // A checkpoint should be usable as a database by itself, but contain the same data as the
        // database it came from.
        let path = TempDbPath::new();
        let db = Db::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        // Write some test data to the database before the checkpoint
        db.put(&cf, "foo", "bar", None)?;

        let checkpoint = db.create_checkpoint(path.path().join("mycheckpoint"), 0)?;

        // Write more data after the checkpoint
        db.put(&cf, "foo", "baz", None)?;

        // Open the checkpoint as if it were a database
        // `false` means the open should not fail if there is a WAL.  Even though `checkpoint`
        // flushes the log, it seems in practice a log file still exists in the checkpoint dir
        let check_db = Db::open_readonly(checkpoint.path(), None, false)?;

        // The value before the checkpoint should be returned
        assert_eq!(
            "bar",
            check_db.get(&cf, "foo", None)?.unwrap().to_string_lossy()
        );

        Ok(())
    }

    #[test]
    fn checkpoint_reports_metrics() -> Result<()> {
        let path = TempDbPath::new();
        let db = Db::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        // Write some test data to the database before the checkpoint
        db.put(&cf, "foo", "bar", None)?;

        let _checkpoint = db.create_checkpoint(path.path().join("mycheckpoint"), 0)?;

        let labels = DatabaseLabels::from(&db);
        assert_eq!(1, CHECKPOINTS_TOTAL.apply_labels(&labels).unwrap().get());
        assert!(CHECKPOINT_FILE_COUNT.apply_labels(&labels).unwrap().get() > 1);
        assert!(CHECKPOINT_SIZE_BYTES.apply_labels(&labels).unwrap().get() > 1);

        Ok(())
    }

    #[ignore] // Sadly this test confirmed my fears that custom CF paths are incompatible with checkpointing
    #[test]
    fn checkpoint_with_custom_cf_paths_is_openable() -> Result<()> {
        // Override the column family data paths with per-CF paths, and see how that effects the
        // structure of the checkpoint.
        let path = TempDbPath::new();
        let foo_path = path.path().join("foo");
        let bar_path = path.path().join("bar");
        let mut options = DbOptions::default();

        options.add_column_family("foo");
        options.set_column_family_path("foo", &foo_path);
        options.add_column_family("bar");
        options.set_column_family_path("bar", &bar_path);

        let db = Db::open(&path, options)?;
        // Write some test data to the database before the checkpoint
        let cf = db.get_cf("foo").unwrap();
        crate::test::fill_db(&db, &cf, 10_000)?;

        let cf = db.get_cf("bar").unwrap();
        crate::test::fill_db(&db, &cf, 10_000)?;

        let checkpoint = db.create_checkpoint(path.path().join("mycheckpoint"), 0)?;

        // Open the checkpoint as if it were a database
        let _check_db = Db::open_readonly(checkpoint.path(), None, false)?;

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
        let path = TempDbPath::new();
        let mut options = DbOptions::default();
        options.add_column_family("foo");
        options.add_column_family("bar");
        let db = Db::open(&path, options)?;
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

        let sequence_number = db.get_latest_sequence_number();

        let checkpoint_path = TempDbPath::new();
        // We don't want this directory to already exist
        std::fs::remove_dir_all(checkpoint_path.path()).unwrap();

        // Take a checkpoint of the DB only live files
        //
        // Since we re-wrote some of the keys and didn't explicitly flush, this will force those
        // writes to be flushed from the memtable to new L0 SSTs, which will be part of the
        // checkpoint.
        let checkpoint = db.create_checkpoint(checkpoint_path.path(), 0)?;

        println!("SST files after checkpoint:");
        dump_live_files(db.get_live_files()?.as_slice());

        // Force a compaction of the live database, that should compact the L0 and L6 levels
        // together.  This change will NOT be reflected in the checkpoint.
        db.compact_all(&foo, None)?;
        db.compact_all(&bar, None)?;

        println!("SST files in live database after checkpoint and compaction:");
        dump_live_files(db.get_live_files()?.as_slice());

        // Open the checkpoint as if it were a database
        let mut options = DbOptions::default();
        options.add_column_family("foo");
        options.add_column_family("bar");
        let check_db = Db::open_readonly(checkpoint.path(), options, false)?;

        assert_eq!(sequence_number, check_db.get_latest_sequence_number());

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

    /// Get the live files from a checkpoing using `Checkpoint::get_live_files`, and also by
    /// opening the checkpoint with `Db` and calling `get_live_files` there.  The resulting list of
    /// files should be identical
    #[test]
    fn checkpoint_live_files_matches_roxide_db_live_files() -> Result<()> {
        let path = TempDbPath::new();
        let mut options = DbOptions::default();
        options.add_column_family("foo");
        options.add_column_family("bar");
        let db = Db::open(&path, options.clone())?;
        let foo = db.get_cf("foo").unwrap();
        let bar = db.get_cf("bar").unwrap();

        // Write some test data to the database before the checkpoint
        crate::test::fill_db_deterministic(&db, &foo, 42, 20_000)?;
        crate::test::fill_db_deterministic(&db, &bar, 52, 20_000)?;
        db.flush_all(None)?;

        // Trigger compaction to organize this data into SSTs
        db.compact_all(&foo, None)?;
        db.compact_all(&bar, None)?;

        // Now re-write half of the keys we wrote before, using a different random value.  This
        // will mean the next compaction will write some new SSTs
        crate::test::fill_db_deterministic(&db, &foo, 42, 10_000)?;
        crate::test::fill_db_deterministic(&db, &bar, 52, 10_000)?;

        let checkpoint_path = TempDbPath::new();
        // We don't want this directory to already exist
        std::fs::remove_dir_all(checkpoint_path.path()).unwrap();

        // Take a checkpoint of the DB
        //
        // Since we re-wrote some of the keys and didn't explicitly flush, and we're specifying a
        // large value for `log_size_for_flush`, this means that some of the writes we made will be
        // not in the SST files but in a WAL in the checkpoint.
        let checkpoint = db.create_checkpoint(checkpoint_path.path(), u64::MAX)?;

        // No changes were made to the live DB since we created this checkpoint, so the sequence
        // numbers should match
        assert_eq!(
            checkpoint.sequence_number(),
            db.get_latest_sequence_number()
        );

        // Query the live files using the checkpoint
        let checkpoint_live_files = checkpoint.get_sst_files()?;

        // Attempt to open the checkpoint read-only and get the live files that way.
        // The results should be the same.
        let checkpoint_db = Db::open_readonly(checkpoint.path(), options, false)?;
        let expected_live_files = checkpoint_db.get_live_files()?;

        assert_eq!(checkpoint_live_files, expected_live_files);

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
