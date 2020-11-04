//! The `Flush` operation triggers a manual compaction, which is the process by which data is
//! consolated and placed into levels.  This isn't normally needed but is useful when we need to
//! force the database to flush for exampel before persisting to S3.
use super::op_metrics;
use super::*;
use crate::db::{self, db::*, opt_txdb::*, txdb::*};
use crate::ffi;
use crate::handle::{self, RocksObject, RocksObjectDefault};
use crate::Result;
use once_cell::sync::OnceCell;
use std::ptr;

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
    ) -> Result<()>;
}

impl Flush for DB {
    fn flush<CF: db::ColumnFamilyLike, O: Into<Option<FlushOptions>>>(
        &self,
        cf: &CF,
        options: O,
    ) -> Result<()> {
        op_metrics::instrument_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::Flush,
            move |_reporter| {
                let options = FlushOptions::from_option(options.into());

                unsafe {
                    ffi_try!(ffi::rocksdb_flush_cf(
                        self.rocks_ptr().as_ptr(),
                        options.rocks_ptr().as_ptr(),
                        cf.rocks_ptr().as_ptr(),
                    ))?;
                }

                Ok(())
            },
        )
    }
}

// I know what you're thinking: "where is the `TransactionDB` impl"?  The Rocks C FFI doesn't have
// a flush function that takes a `rocksdb_transactiondb_t` pointer.  I'm pretty sure it's
// supported in the C++ code but it somehow fell through the cracks in the C bindings.  The only
// reason it works on `OptimisticTransactionDB` is because it's possible to get the base DB handle
// for an optimistic db.
//
// For now there's a dummy impl that panics; someday a proper implementation needs to be provided
impl Flush for TransactionDB {
    fn flush<CF: db::ColumnFamilyLike, O: Into<Option<FlushOptions>>>(
        &self,
        _cf: &CF,
        _options: O,
    ) -> Result<()> {
        unimplemented!()
    }
}

impl Flush for OptimisticTransactionDB {
    fn flush<CF: db::ColumnFamilyLike, O: Into<Option<FlushOptions>>>(
        &self,
        cf: &CF,
        options: O,
    ) -> Result<()> {
        let options = FlushOptions::from_option(options.into());

        unsafe {
            Self::with_base_db(self.handle(), |base_db| {
                ffi_try!(ffi::rocksdb_flush_cf(
                    base_db,
                    options.rocks_ptr().as_ptr(),
                    cf.rocks_ptr().as_ptr(),
                ))
            })?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::DBLike;
    use crate::ops::{DBOpen, Put};
    use crate::test::TempDBPath;

    #[test]
    fn db_flush() -> Result<()> {
        let path = TempDBPath::new();
        let db = DB::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        db.put(&cf, "foo", "bar", None)?;

        db.flush(&cf, None)?;

        Ok(())
    }

    #[test]
    fn opt_txdb_flush() -> Result<()> {
        let path = TempDBPath::new();
        let db = OptimisticTransactionDB::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        db.put(&cf, "foo", "bar", None)?;

        db.flush(&cf, None)?;

        Ok(())
    }
}
