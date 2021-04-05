//! The `Flush` operation triggers a manual compaction, which is the process by which data is
//! consolated and placed into levels.  This isn't normally needed but is useful when we need to
//! force the database to flush for exampel before persisting to S3.
use super::op_metrics;
use super::*;
use crate::db::{self, DbLike};
use crate::ffi;
use crate::handle::{self, RocksObject, RocksObjectDefault};
use crate::status;
use crate::Result;
use once_cell::sync::OnceCell;
use std::ptr;

cpp! {{
    #include "src/ops/flush.h"
    #include "src/ops/flush.cpp"
}}

extern "C" {
    fn flush_db(
        db_ptr: *mut libc::c_void,
        options: *const ffi::rocksdb_flushoptions_t,
        cfs: *mut *mut ffi::rocksdb_column_family_handle_t,
        cf_len: libc::size_t,
    ) -> status::CppStatus;
}

/// The options specific to a `Flush` operation
///
/// TODO: If we need to set any of these options, implement that
pub struct FlushOptions {
    inner: ptr::NonNull<ffi::rocksdb_flushoptions_t>,
}

impl FlushOptions {
    fn new() -> Self {
        FlushOptions {
            inner: unsafe { ptr::NonNull::new(ffi::rocksdb_flushoptions_create()).unwrap() },
        }
    }
}

impl Drop for FlushOptions {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_flushoptions_destroy(self.inner.as_ptr());
        }
    }
}

impl Default for FlushOptions {
    fn default() -> Self {
        FlushOptions::new()
    }
}

// Technically the struct which RocksDB allocates when a `FlushOptions` is created is not safe for
// concurrent thread access, meaning it's `Send` but not `Sync` in Rust terminology.  However, at
// the Rust level we don't expose any operations on the struct, it's just a placeholder because
// Rocks doesn't allow us to pass a NULL pointer for options.  This in this implementation it's
// safe for both `Send` and `Sync`
unsafe impl Sync for FlushOptions {}
unsafe impl Send for FlushOptions {}

impl handle::RocksObject<ffi::rocksdb_flushoptions_t> for FlushOptions {
    fn rocks_ptr(&self) -> ptr::NonNull<ffi::rocksdb_flushoptions_t> {
        self.inner
    }
}

impl handle::RocksObjectDefault<ffi::rocksdb_flushoptions_t> for FlushOptions {
    fn default_object() -> &'static Self {
        static DEFAULT_FLUSH_OPTIONS: OnceCell<FlushOptions> = OnceCell::new();
        DEFAULT_FLUSH_OPTIONS.get_or_init(FlushOptions::default)
    }
}

pub trait Flush: RocksOpBase {
    /// Forces a column family to flush all data to disk
    fn flush<CF: db::ColumnFamilyLike, O: Into<Option<FlushOptions>>>(
        &self,
        cf: &CF,
        options: O,
    ) -> Result<()> {
        self.flush_cfs(std::slice::from_ref(cf), options)
    }

    /// Forces multiple column families to flush all data to disk.
    ///
    /// if atomic flush is enabled, then the flush will either flush all column families
    /// successfully, or fail
    fn flush_cfs<CF: db::ColumnFamilyLike, O: Into<Option<FlushOptions>>>(
        &self,
        cfs: &[CF],
        options: O,
    ) -> Result<()>;

    /// Flush all column families in this database.
    ///
    /// if atomic flush is enabled, then the flush will either flush all column families
    /// successfully, or fail
    fn flush_all<O: Into<Option<FlushOptions>>>(&self, options: O) -> Result<()>
    where
        Self: DbLike,
    {
        let cf_names = self.get_cf_names();
        let cfs = cf_names
            .into_iter()
            .map(|cf_name| {
                self.get_cf(cf_name).unwrap_or_else(|| {
                    panic!("BUG: get_cf_names returned CF '{}', but call to `get_cf` with that name failed",
                        cf_name);
                })
            })
            .collect::<Vec<_>>();

        self.flush_cfs(&cfs, options)
    }
}

impl<T: DbLike + GetDbPtr> Flush for T {
    fn flush_cfs<CF: db::ColumnFamilyLike, O: Into<Option<FlushOptions>>>(
        &self,
        cfs: &[CF],
        options: O,
    ) -> Result<()> {
        op_metrics::instrument_db_op(self, op_metrics::DatabaseOperation::Flush, move || {
            let options = FlushOptions::from_option(options.into());

            let mut cf_ptrs = cfs
                .iter()
                .map(|cf| cf.rocks_ptr().as_ptr())
                .collect::<Vec<_>>();

            let status = unsafe {
                flush_db(
                    self.get_db_ptr(),
                    options.rocks_ptr().as_ptr(),
                    cf_ptrs.as_mut_ptr(),
                    cf_ptrs.len() as libc::size_t,
                )
                .into_status()
            };

            if status.is_ok() {
                // Operation succeeded
                Ok(())
            } else {
                // Error
                status.into_result()?;
                unreachable!()
            }
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::{db::*, opt_txdb::*, txdb::*};
    use crate::ops::{DbOpen, Put};
    use crate::test::TempDbPath;

    #[test]
    fn db_flush() -> Result<()> {
        let path = TempDbPath::new();
        let db = Db::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        db.put(&cf, "foo", "bar", None)?;

        db.flush(&cf, None)?;
        db.flush_all(None)?;

        Ok(())
    }

    #[test]
    fn txdb_flush() -> Result<()> {
        let path = TempDbPath::new();
        let db = TransactionDb::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        db.put(&cf, "foo", "bar", None)?;

        db.flush(&cf, None)?;
        db.flush_all(None)?;

        Ok(())
    }

    #[test]
    fn opt_txdb_flush() -> Result<()> {
        let path = TempDbPath::new();
        let db = OptimisticTransactionDb::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        db.put(&cf, "foo", "bar", None)?;

        db.flush(&cf, None)?;
        db.flush_all(None)?;

        Ok(())
    }

    /// This isn't really related to the Flush operation, but it's to prove to myself that Rocks
    /// will still persist data to disk if the database object is dropped before an explicit Flush.
    ///
    /// I'm seeing behavior that makes me doubt that and I want to be reassured.
    #[test]
    fn flush_not_required_before_close() -> Result<()> {
        let path = TempDbPath::new();

        {
            let db = Db::open(&path, None)?;
            let cf = db.get_cf("default").unwrap();

            db.put(&cf, "foo", "bar", None)?;
        }

        // `db` and `cf` went out of scope do the database was closed
        let db = Db::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();
        assert_eq!("bar", db.get(&cf, "foo", None)?.unwrap().to_string_lossy());

        Ok(())
    }
}
