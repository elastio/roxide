//! The `Compact` operation triggers a manual compaction, which is the process by which data is
//! consolated and placed into levels.  This isn't normally needed but is useful when we need to
//! force the database to compact for exampel before persisting to S3.
use super::op_metrics;
use super::*;
use crate::db::{self, db::*, opt_txdb::*, txdb::*};
use crate::ffi;
use crate::handle::{RocksObject, RocksObjectDefault};
use crate::Result;
use rocksdb::CompactOptions;

pub trait Compact: RocksOpBase {
    /// Compacts a a range of keys or all keys in a column family
    fn compact_range<CF: db::ColumnFamilyLike, K: BinaryStr, O: Into<Option<CompactOptions>>>(
        &self,
        cf: &CF,
        key_range: OpenKeyRange<K>,
        options: O,
    ) -> Result<()>;

    fn compact_all<CF: db::ColumnFamilyLike, O: Into<Option<CompactOptions>>>(
        &self,
        cf: &CF,
        options: O,
    ) -> Result<()> {
        self.compact_range(cf, key_range_all(), options)
    }
}

impl Compact for Db {
    fn compact_range<CF: db::ColumnFamilyLike, K: BinaryStr, O: Into<Option<CompactOptions>>>(
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

                let options = CompactOptions::from_option(options.into());

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
        O: Into<Option<CompactOptions>>,
    >(
        &self,
        _cf: &CF,
        _key_range: OpenKeyRange<K>,
        _options: O,
    ) -> Result<()> {
        unimplemented!()
    }
}

impl Compact for OptimisticTransactionDb {
    fn compact_range<
        'a,
        CF: db::ColumnFamilyLike,
        K: BinaryStr,
        O: Into<Option<CompactOptions>>,
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

                let options = CompactOptions::from_option(options.into());

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
    fn opt_txdb_compact() -> Result<()> {
        let path = TempDbPath::new();
        let db = OptimisticTransactionDb::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        db.put(&cf, "foo", "bar", None)?;

        db.compact_all(&cf, None)?;

        Ok(())
    }
}
