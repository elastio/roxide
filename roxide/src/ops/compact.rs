//! The `Compact` operation triggers a manual compaction, which is the process by which data is
//! consolated and placed into levels.  This isn't normally needed but is useful when we need to
//! force the database to compact for exampel before persisting to S3.
use super::op_metrics;
use super::*;
use crate::db::{self, db::*, opt_txdb::*, txdb::*};
use crate::handle::{self, RocksObject, RocksObjectDefault};
use crate::Result;
use crate::{ffi, RocksIterator};
use crate::{status, CppStatus};
use once_cell::sync::OnceCell;
use std::ffi::CString;
use std::ptr;

cpp! {{
    #include "src/ops/compact_files.h"
    #include "src/ops/compact_files.cpp"
}}

extern "C" {
    fn rocksdb_compact_files(
        db_ptr: *mut libc::c_void,
        options: *const ffi::rocksdb_compactoptions_t,
        cf: *const ffi::rocksdb_column_family_handle_t,
        output_level: u32,
        file_names: *const *const libc::c_char,
        fn_len: libc::size_t,
    ) -> status::CppStatus;
}

/// The options specific to a `Compact` operation
///
/// TODO: If we need to set any of these options, implement that
pub struct CompactionOptions {
    inner: ptr::NonNull<ffi::rocksdb_compactoptions_t>,
}

impl CompactionOptions {
    fn new() -> Self {
        CompactionOptions {
            inner: unsafe { ptr::NonNull::new(ffi::rocksdb_compactoptions_create()).unwrap() },
        }
    }
}

impl Drop for CompactionOptions {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_compactoptions_destroy(self.inner.as_ptr());
        }
    }
}

impl Default for CompactionOptions {
    fn default() -> Self {
        CompactionOptions::new()
    }
}

// Technically the struct which RocksDB allocates when a `CompactionOptions` is created is not safe for
// concurrent thread access, meaning it's `Send` but not `Sync` in Rust terminology.  However, at
// the Rust level we don't expose any operations on the struct, it's just a placeholder because
// Rocks doesn't allow us to pass a NULL pointer for options.  This in this implementation it's
// safe for both `Send` and `Sync`
unsafe impl Sync for CompactionOptions {}
unsafe impl Send for CompactionOptions {}

impl handle::RocksObject<ffi::rocksdb_compactoptions_t> for CompactionOptions {
    fn rocks_ptr(&self) -> ptr::NonNull<ffi::rocksdb_compactoptions_t> {
        self.inner
    }
}

impl handle::RocksObjectDefault<ffi::rocksdb_compactoptions_t> for CompactionOptions {
    fn default_object() -> &'static Self {
        static DEFAULT_COMPACTION_OPTIONS: OnceCell<CompactionOptions> = OnceCell::new();
        DEFAULT_COMPACTION_OPTIONS.get_or_init(CompactionOptions::default)
    }
}

pub trait Compact: RocksOpBase + crate::ops::iterate::IterateAll {
    /// Compacts a a range of keys or all keys in a column family
    fn compact_range<CF: db::ColumnFamilyLike, K: BinaryStr, O: Into<Option<CompactionOptions>>>(
        &self,
        cf: &CF,
        key_range: OpenKeyRange<K>,
        options: O,
    ) -> Result<()>;

    fn compact_all<CF: db::ColumnFamilyLike, O: Into<Option<CompactionOptions>>>(
        &self,
        cf: &CF,
        options: O,
    ) -> Result<()> {
        let mut iterator = self.iterate_all(cf, None)?;
        unsafe {
            let start = Vec::from(iterator.get_current()?.unwrap().0);
            iterator.seek_to_last();
            let end = Vec::from(iterator.get_current()?.unwrap().0);

            let range = OpenKeyRange::KeysBetween(start, end);
            self.compact_range(cf, range, options)
        }
    }

    fn compact_files<CF: db::ColumnFamilyLike, O: Into<Option<CompactionOptions>>>(
        &self,
        cf: &CF,
        file_names: Vec<String>,
        output_level: u32,
        options: O,
    ) -> Result<CppStatus>;
}

impl Compact for Db {
    fn compact_range<CF: db::ColumnFamilyLike, K: BinaryStr, O: Into<Option<CompactionOptions>>>(
        &self,
        cf: &CF,
        key_range: OpenKeyRange<K>,
        options: O,
    ) -> Result<()> {
        op_metrics::instrument_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::CompactRange,
            move |_reporter| {
                let (start_key_ptr, start_key_len) = key_range.start().as_ptr_and_len();
                let (end_key_ptr, end_key_len) = key_range.end().as_ptr_and_len();

                let options = CompactionOptions::from_option(options.into());

                unsafe {
                    ffi::rocksdb_compact_range_cf_opt(
                        self.rocks_ptr().as_ptr(),
                        cf.rocks_ptr().as_ptr(),
                        options.rocks_ptr().as_ptr(),
                        start_key_ptr as *const libc::c_char,
                        start_key_len as libc::size_t,
                        end_key_ptr as *const libc::c_char,
                        end_key_len as libc::size_t,
                    );
                }

                Ok(())
            },
        )
    }

    fn compact_files<CF: db::ColumnFamilyLike, O: Into<Option<CompactionOptions>>>(
        &self,
        cf: &CF,
        file_names: Vec<String>,
        output_level: u32,
        options: O,
    ) -> Result<CppStatus> {
        unreachable!();
    }
}

// I know what you're thinking: "where is the `TransactionDb` impl"?  The Rocks C FFI doesn't have
// a compact range function that takes a `rocksdb_transactiondb_t` pointer.  I'm pretty sure it's
// supported in the C++ code but it somehow fell through the cracks in the C bindings.  The only
// reason it works on `OptimisticTransactionDb` is because it's possible to get the base Db handle
// for an optimistic db.
//
// For now there's a dummy impl that panics; someday a proper implementation needs to be provided
impl Compact for TransactionDb {
    fn compact_range<
        'a,
        CF: db::ColumnFamilyLike,
        K: BinaryStr,
        O: Into<Option<CompactionOptions>>,
    >(
        &self,
        _cf: &CF,
        _key_range: OpenKeyRange<K>,
        _options: O,
    ) -> Result<()> {
        unimplemented!()
    }

    fn compact_files<CF: db::ColumnFamilyLike, O: Into<Option<CompactionOptions>>>(
        &self,
        cf: &CF,
        file_names: Vec<String>,
        output_level: u32,
        options: O,
    ) -> Result<CppStatus> {
        unreachable!();
    }
}

impl Compact for OptimisticTransactionDb {
    fn compact_range<
        'a,
        CF: db::ColumnFamilyLike,
        K: BinaryStr,
        O: Into<Option<CompactionOptions>>,
    >(
        &self,
        cf: &CF,
        key_range: OpenKeyRange<K>,
        options: O,
    ) -> Result<()> {
        op_metrics::instrument_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::CompactRange,
            move |_reporter| {
                let (start_key_ptr, start_key_len) = key_range.start().as_ptr_and_len();
                let (end_key_ptr, end_key_len) = key_range.end().as_ptr_and_len();

                let options = CompactionOptions::from_option(options.into());

                unsafe {
                    Self::with_base_db(self.handle(), |base_db| {
                        ffi::rocksdb_compact_range_cf_opt(
                            base_db,
                            cf.rocks_ptr().as_ptr(),
                            options.rocks_ptr().as_ptr(),
                            start_key_ptr as *const libc::c_char,
                            start_key_len as libc::size_t,
                            end_key_ptr as *const libc::c_char,
                            end_key_len as libc::size_t,
                        );

                        Ok(())
                    })
                }
            },
        )
    }

    fn compact_files<CF: db::ColumnFamilyLike, O: Into<Option<CompactionOptions>>>(
        &self,
        cf: &CF,
        file_names: Vec<String>,
        output_level: u32,
        options: O,
    ) -> Result<CppStatus> {
        let options = CompactionOptions::from_option(options.into());

        let cs_file_names = file_names
            .iter()
            .map(|x| CString::new(x.clone()).unwrap())
            .collect::<Vec<_>>();

        let znames = cs_file_names.iter().map(|x| x.as_ptr()).collect::<Vec<_>>();

        let status = unsafe {
            rocksdb_compact_files(
                self.get_db_ptr(),
                options.rocks_ptr().as_ptr(),
                cf.rocks_ptr().as_ptr(),
                output_level,
                znames.as_ptr(),
                file_names.len(),
            )
        };

        Ok(status)
    }
}

#[cfg(test)]
mod test {
    use super::super::put::Put;
    use super::*;
    use crate::db::DbLike;
    use crate::test::TempDbPath;

    #[test]
    fn db_compact() -> Result<()> {
        let path = TempDbPath::new();
        let db = Db::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        db.put(&cf, "foo", "bar", None)?;

        db.compact_all(&cf, None)?;

        Ok(())
    }

    #[test]
    fn opt_txdb_compact_files() -> Result<()> {
        let path = TempDbPath::new();
        let db = OptimisticTransactionDb::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        db.put(&cf, "foo", "bar", None)?;
        db.compact_all(&cf, None)?;

        let live_files = db.get_live_files()?;

        let res = db.compact_files(
            &cf,
            live_files
                .iter()
                .map(|x| {
                    x.file_path
                        .file_name()
                        .unwrap()
                        .to_string_lossy()
                        .to_string()
                })
                .collect(),
            6,
            None,
        );

        assert!(res.is_ok());
        Ok(())
    }

    #[test]
    fn opt_txdb_compact() -> Result<()> {
        let path = TempDbPath::new();
        let db = OptimisticTransactionDb::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        db.put(&cf, "foo", "bar", None)?;

        db.compact_all(&cf, None)?;

        Ok(())
    }
}
