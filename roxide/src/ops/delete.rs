//! The `Delete` operation deletes a single key or range of keys from the database.
use super::op_metrics;
use super::*;
use crate::db::{self, db::*, opt_txdb::*, txdb::*};
use crate::db_options::WriteOptions;
use crate::ffi;
use crate::handle::{RocksObject, RocksObjectDefault};
use crate::status;
use crate::tx::{sync, unsync};
use crate::write_batch::WriteBatch;
use crate::{Error, Result};
use std::ptr::NonNull;

cpp! {{
    #include "src/ops/delete.h"
    #include "src/ops/delete.cpp"
}}

// Declare the C helper code that supports DeleteRange
extern "C" {
    fn delete_range_db(
        db: *const libc::c_void,
        options: *const ffi::rocksdb_writeoptions_t,
        cf: *const ffi::rocksdb_column_family_handle_t,
        start_key_ptr: *const libc::c_char,
        start_key_len: libc::size_t,
        end_key_ptr: *const libc::c_char,
        end_key_len: libc::size_t,
    ) -> status::CppStatus;
}

/// Provides methods to delete a single key or a collection of keys.
///
/// If deleting a range of keys, it's much more efficient to use the `DeleteRange` trait's methods,
/// where available.
pub trait Delete: RocksOpBase {
    /// Raw version of `delete`, operating on unsafe C bindings.  Not intended for use by external
    /// crates.
    ///
    /// # Safety
    ///
    /// Never call this function; it's intended for internal use by the `roxide`
    /// implementation.
    unsafe fn raw_delete(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        key: &[u8],
        options: NonNull<ffi::rocksdb_writeoptions_t>,
    ) -> Result<()>;

    /// Deletes `key` from `cf`
    fn delete(
        &self,
        cf: &impl db::ColumnFamilyLike,
        key: impl BinaryStr,
        options: impl Into<Option<WriteOptions>>,
    ) -> Result<()> {
        op_metrics::instrument_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::Delete,
            move |reporter| {
                let options = WriteOptions::from_option(options.into());

                unsafe {
                    reporter.processing_item(key.as_slice().len(), None, || {
                        Self::raw_delete(
                            self.handle(),
                            cf.rocks_ptr(),
                            key.as_slice(),
                            options.rocks_ptr(),
                        )
                    })
                }
            },
        )
    }

    /// Deletes a collection of keys from `cf`.  Some implementations of this method will have a
    /// RocksDB native vectorized implementation which may be faster for batch deletes.
    fn multi_delete<CF: db::ColumnFamilyLike, K: BinaryStr>(
        &self,
        cf: &CF,
        keys: impl IntoIterator<Item = K>,
        options: impl Into<Option<WriteOptions>>,
    ) -> Result<()> {
        op_metrics::instrument_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::MultiDelete,
            move |reporter| {
                let options = WriteOptions::from_option(options.into());

                let keys = keys.into_iter();
                let results = keys.map(|key| unsafe {
                    reporter.processing_item(key.as_slice().len(), None, || {
                        Self::raw_delete(
                            self.handle(),
                            cf.rocks_ptr(),
                            key.as_slice(),
                            options.rocks_ptr(),
                        )
                    })
                });

                // Contort ourselves a bit to pull out the per-item `Result` into one top-level `Result`
                // which will contain whatever the first error was if any
                let results: Result<Vec<_>> = results.collect();

                results.map(|_| ())
            },
        )
    }

    /// Async version of `multi_delete`
    fn async_delete<CF: db::ColumnFamilyLike, K: BinaryStr + Send + 'static>(
        &self,
        cf: &CF,
        keys: impl IntoIterator<Item = K> + Send + 'static,
        options: impl Into<Option<WriteOptions>>,
    ) -> op_metrics::AsyncOpFuture<()>
    where
        <Self as RocksOpBase>::HandleType: Sync + Send,
    {
        op_metrics::instrument_async_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::AsyncDelete,
            move |reporter| {
                let handle = self.handle().clone();
                let cf = cf.clone();
                let options = WriteOptions::from_option(options.into());

                reporter.run_blocking_task(move |reporter| {
                    let keys = keys.into_iter();
                    let results = keys.map(|key| unsafe {
                        reporter.processing_item(key.as_slice().len(), None, || {
                            Self::raw_delete(
                                &handle,
                                cf.rocks_ptr(),
                                key.as_slice(),
                                options.rocks_ptr(),
                            )
                        })
                    });

                    // Contort ourselves a bit to pull out the per-item `Result` into one top-level `Result`
                    // which will contain whatever the first error was if any
                    let results: Result<Vec<_>> = results.collect();

                    results.map(|_| ())
                })
            },
        )
    }

    /// Like `async_delete`, but operates on a single scalar value.
    ///
    /// It's not clear if this is a good approach.  Is the async overhead too high for a single
    /// record?  More testing is needed.
    fn async_delete_scalar<CF: db::ColumnFamilyLike, K: BinaryStr + Send + 'static>(
        &self,
        cf: &CF,
        key: K,
        options: impl Into<Option<WriteOptions>>,
    ) -> op_metrics::AsyncOpFuture<()>
    where
        <Self as RocksOpBase>::HandleType: Sync + Send,
    {
        op_metrics::instrument_async_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::AsyncDelete,
            move |reporter| {
                let handle = self.handle().clone();
                let cf = cf.clone();
                let options = WriteOptions::from_option(options.into());

                reporter.run_blocking_task(move |reporter| {
                    unsafe {
                        reporter.processing_item(key.as_slice().len(), None, || {
                            Self::raw_delete(
                                &handle,
                                cf.rocks_ptr(),
                                key.as_slice(),
                                options.rocks_ptr(),
                            )
                        })?
                    };

                    Ok(())
                })
            },
        )
    }
}

/// Provides methods to delete a range of keys.  Whenever possible it's preferrable to use range
/// deletes over point deletes, for performance reasons.
pub trait DeleteRange: RocksOpBase {
    /// Raw version of `delete_range`, operating on unsafe C bindings.  Not intended for use by external
    /// crates.
    ///
    /// # Safety
    ///
    /// Never call this function; it's intended for internal use by the `roxide`
    /// implementation.
    unsafe fn raw_delete_range(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        start: &[u8],
        end: &[u8],
        options: NonNull<ffi::rocksdb_writeoptions_t>,
    ) -> Result<()>;

    /// Deletes a range of keys from `cf`.  This is the most efficient way to delete multiple keys
    /// as it produces only one tombstone value regardless of the size of the range.
    fn delete_range<
        CF: db::ColumnFamilyLike,
        K: BinaryStr,
        KR: Into<KeyRange<K>>,
        O: Into<Option<WriteOptions>>,
    >(
        &self,
        cf: &CF,
        key_range: KR,
        options: O,
    ) -> Result<()> {
        op_metrics::instrument_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::DeleteRange,
            move |reporter| {
                let options = WriteOptions::from_option(options.into());
                let key_range = key_range.into();

                unsafe {
                    reporter.processing_item(key_range.start().as_slice().len(), None, || {
                        Self::raw_delete_range(
                            self.handle(),
                            cf.rocks_ptr(),
                            key_range.start().as_slice(),
                            key_range.end().as_slice(),
                            options.rocks_ptr(),
                        )
                    })
                }
            },
        )
    }

    /// Deletes one or more key ranges from the database.
    fn multi_delete_range<
        CF: db::ColumnFamilyLike,
        K: BinaryStr,
        ITEM: Into<KeyRange<K>>,
        I: IntoIterator<Item = ITEM>,
        O: Into<Option<WriteOptions>>,
    >(
        &self,
        cf: &CF,
        ranges: I,
        options: O,
    ) -> Result<()> {
        op_metrics::instrument_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::MultiDeleteRange,
            move |reporter| {
                let options = WriteOptions::from_option(options.into());

                let ranges = ranges.into_iter();
                let results = ranges.map(|range| unsafe {
                    let range = range.into();
                    reporter.processing_item(range.start().as_slice().len(), None, || {
                        Self::raw_delete_range(
                            self.handle(),
                            cf.rocks_ptr(),
                            range.start().as_slice(),
                            range.end().as_slice(),
                            options.rocks_ptr(),
                        )
                    })
                });

                // Contort ourselves a bit to pull out the per-item `Result` into one top-level `Result`
                // which will contain whatever the first error was if any
                let results: Result<Vec<_>> = results.collect();

                results.map(|_| ())
            },
        )
    }

    /// Async version of `multi_delete_range`
    fn async_delete_range<
        CF: db::ColumnFamilyLike,
        K: BinaryStr + Send + 'static,
        ITEM: Into<KeyRange<K>>,
        I: IntoIterator<Item = ITEM> + Send + 'static,
        O: Into<Option<WriteOptions>>,
    >(
        &self,
        cf: &CF,
        ranges: I,
        options: O,
    ) -> op_metrics::AsyncOpFuture<()>
    where
        <Self as RocksOpBase>::HandleType: Sync + Send,
    {
        op_metrics::instrument_async_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::AsyncDeleteRange,
            move |reporter| {
                let handle = self.handle().clone();
                let cf = cf.clone();
                let options = WriteOptions::from_option(options.into());

                reporter.run_blocking_task(move |reporter| {
                    let ranges = ranges.into_iter();
                    let results = ranges.map(|range| unsafe {
                        let range = range.into();
                        reporter.processing_item(range.start().as_slice().len(), None, || {
                            Self::raw_delete_range(
                                &handle,
                                cf.rocks_ptr(),
                                range.start().as_slice(),
                                range.end().as_slice(),
                                options.rocks_ptr(),
                            )
                        })
                    });

                    // Contort ourselves a bit to pull out the per-item `Result` into one top-level `Result`
                    // which will contain whatever the first error was if any
                    let results: Result<Vec<_>> = results.collect();

                    results.map(|_| ())
                })
            },
        )
    }

    /// Like `async_delete_range`, but operates on a single scalar value.
    ///
    /// It's not clear if this is a good approach.  Is the async overhead too high for a single
    /// record?  More testing is needed.
    fn async_delete_range_scalar<
        CF: db::ColumnFamilyLike,
        K: BinaryStr + Send + 'static,
        RangeT: Into<KeyRange<K>> + Send + 'static,
    >(
        &self,
        cf: &CF,
        range: RangeT,
        options: impl Into<Option<WriteOptions>>,
    ) -> op_metrics::AsyncOpFuture<()>
    where
        <Self as RocksOpBase>::HandleType: Sync + Send,
    {
        op_metrics::instrument_async_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::AsyncDeleteRange,
            move |reporter| {
                let handle = self.handle().clone();
                let cf = cf.clone();
                let options = WriteOptions::from_option(options.into());

                reporter.run_blocking_task(move |reporter| {
                    unsafe {
                        let range = range.into();
                        reporter.processing_item(range.start().as_slice().len(), None, || {
                            Self::raw_delete_range(
                                &handle,
                                cf.rocks_ptr(),
                                range.start().as_slice(),
                                range.end().as_slice(),
                                options.rocks_ptr(),
                            )
                        })?
                    };

                    Ok(())
                })
            },
        )
    }
}

impl Delete for Db {
    unsafe fn raw_delete(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        key: &[u8],
        options: NonNull<ffi::rocksdb_writeoptions_t>,
    ) -> Result<()> {
        ffi_try!(ffi::rocksdb_delete_cf(
            handle.rocks_ptr().as_ptr(),
            options.as_ptr(),
            cf.as_ptr(),
            key.as_ptr() as *const libc::c_char,
            key.len() as libc::size_t,
        ))
    }
}

impl Delete for TransactionDb {
    unsafe fn raw_delete(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        key: &[u8],
        options: NonNull<ffi::rocksdb_writeoptions_t>,
    ) -> Result<()> {
        ffi_try!(ffi::rocksdb_transactiondb_delete_cf(
            handle.rocks_ptr().as_ptr(),
            options.as_ptr(),
            cf.as_ptr(),
            key.as_ptr() as *const libc::c_char,
            key.len() as libc::size_t,
        ))
    }
}

impl Delete for OptimisticTransactionDb {
    unsafe fn raw_delete(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        key: &[u8],
        options: NonNull<ffi::rocksdb_writeoptions_t>,
    ) -> Result<()> {
        // The optimistic transaction DB API is slightly different than either of the others.
        // See the comment in `put.rs` for the details
        Self::with_base_db(handle, |base_db| {
            ffi_try!(ffi::rocksdb_delete_cf(
                base_db,
                options.as_ptr(),
                cf.as_ptr(),
                key.as_ptr() as *const libc::c_char,
                key.len() as libc::size_t,
            ))
        })
    }
}

impl Delete for unsync::Transaction {
    unsafe fn raw_delete(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        key: &[u8],
        _options: NonNull<ffi::rocksdb_writeoptions_t>,
    ) -> Result<()> {
        ffi_try!(ffi::rocksdb_transaction_delete_cf(
            handle.rocks_ptr().as_ptr(),
            cf.as_ptr(),
            key.as_ptr() as *const libc::c_char,
            key.len() as libc::size_t,
        ))
    }

    fn delete(
        &self,
        cf: &impl db::ColumnFamilyLike,
        key: impl BinaryStr,
        options: impl Into<Option<WriteOptions>>,
    ) -> Result<()> {
        if options.into().is_some() {
            return Err(Error::other_error(
                "Write options for `Delete` operations on a `Transaction` are not supported",
            ));
        }

        unsafe {
            Self::raw_delete(
                self.handle(),
                cf.rocks_ptr(),
                key.as_slice(),
                WriteOptions::from_option(None).rocks_ptr(),
            )
        }
    }
}

// The implementation for the thread-safe `Transaction` is just a wrapper around the
// `unsync::Transaction` implementation so it takes a rather different form.
impl Delete for sync::Transaction {
    unsafe fn raw_delete(
        _handle: &Self::HandleType,
        _cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        _key: &[u8],
        _options: NonNull<ffi::rocksdb_writeoptions_t>,
    ) -> Result<()> {
        unreachable!()
    }

    fn delete(
        &self,
        cf: &impl db::ColumnFamilyLike,
        key: impl BinaryStr,
        options: impl Into<Option<WriteOptions>>,
    ) -> Result<()> {
        self.with_tx(move |tx| tx.delete(cf, key, options))
    }

    fn multi_delete<CF: db::ColumnFamilyLike, K: BinaryStr>(
        &self,
        cf: &CF,
        keys: impl IntoIterator<Item = K>,
        options: impl Into<Option<WriteOptions>>,
    ) -> Result<()> {
        self.with_tx(move |tx| tx.multi_delete(cf, keys, options))
    }

    /// Async version of `multi_delete`
    fn async_delete<CF: db::ColumnFamilyLike, K: BinaryStr + Send + 'static>(
        &self,
        cf: &CF,
        keys: impl IntoIterator<Item = K> + Send + 'static,
        options: impl Into<Option<WriteOptions>>,
    ) -> op_metrics::AsyncOpFuture<()>
    where
        <Self as RocksOpBase>::HandleType: Sync + Send,
    {
        let options = options.into();
        let cf_clone = cf.clone();

        // the async version isn't available on `unsync::Transaction`; that's almost the entire
        // reason for having this `sync` version.  Instead, just call the sync version from a
        // worker thread
        op_metrics::instrument_async_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::AsyncDelete,
            move |_reporter| {
                self.async_with_tx(move |tx| tx.multi_delete(&cf_clone, keys, options))
            },
        )
    }
}

impl Delete for WriteBatch {
    unsafe fn raw_delete(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        key: &[u8],
        _options: NonNull<ffi::rocksdb_writeoptions_t>,
    ) -> Result<()> {
        ffi::rocksdb_writebatch_delete_cf(
            handle.rocks_ptr().as_ptr(),
            cf.as_ptr(),
            key.as_ptr() as *const libc::c_char,
            key.len() as libc::size_t,
        );

        Ok(())
    }

    fn delete(
        &self,
        cf: &impl db::ColumnFamilyLike,
        key: impl BinaryStr,
        options: impl Into<Option<WriteOptions>>,
    ) -> Result<()> {
        if options.into().is_some() {
            return Err(Error::other_error(
                "Write options for `Delete` operations on a `WriteBatch` are not supported",
            ));
        }

        unsafe {
            Self::raw_delete(
                self.handle(),
                cf.rocks_ptr(),
                key.as_slice(),
                WriteOptions::from_option(None).rocks_ptr(),
            )
        }
    }
}

/// Implement DeleteRange for Db
///
/// Per the RocksDB HISTORY.MD:
///
/// Since 6.15.0, TransactionDb returns error Statuses from calls to DeleteRange() and calls to
/// Write() where the WriteBatch contains a range deletion. Previously such operations may have
/// succeeded while not providing the expected transactional guarantees. There are certain cases
/// where range deletion can still be used on such DBs; see the API doc on
/// TransactionDB::DeleteRange() for details.
///
/// Note that I can't find that API doc so DeleteRange is basicallynot possible
impl DeleteRange for Db {
    unsafe fn raw_delete_range(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        start: &[u8],
        end: &[u8],
        options: NonNull<ffi::rocksdb_writeoptions_t>,
    ) -> Result<()> {
        delete_range_db(
            Self::raw_get_db_ptr(handle),
            options.as_ptr(),
            cf.as_ptr(),
            start.as_ptr() as *const libc::c_char,
            start.len() as libc::size_t,
            end.as_ptr() as *const libc::c_char,
            end.len() as libc::size_t,
        )
        .into_result()?;

        Ok(())
    }
}

/// The comment above on `DB` is correct, however when using OptimisticTransactionDb there is an
/// escape hatch, one can use the underlying database without transaction semantics.  Since there
/// are times when one can accept that the range delete is not transactional but nonetheless wants
/// a range delete and not a bunch of single record delete tombstones, this is still useful.
impl DeleteRange for OptimisticTransactionDb {
    unsafe fn raw_delete_range(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        start: &[u8],
        end: &[u8],
        options: NonNull<ffi::rocksdb_writeoptions_t>,
    ) -> Result<()> {
        Self::with_base_db(handle, |base_db| {
            let db_ptr = cpp!([base_db as "::rocksdb_t*"] -> *mut libc::c_void as "rocksdb::DB*" {
                return cast_to_db(base_db);
            });

            delete_range_db(
                db_ptr,
                options.as_ptr(),
                cf.as_ptr(),
                start.as_ptr() as *const libc::c_char,
                start.len() as libc::size_t,
                end.as_ptr() as *const libc::c_char,
                end.len() as libc::size_t,
            )
            .into_result()?;

            Ok(())
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::ColumnFamilyLike;
    use crate::db::DbLike;
    use crate::test::TempDbPath;
    use crate::test::{random_key, random_keys};
    use more_asserts::*;

    fn delete_test_impl<DBT: DbLike + Get + Put + Delete, CFT: ColumnFamilyLike>(
        db: &DBT,
        cf: &CFT,
    ) -> Result<()> {
        let key = random_key();

        db.put(cf, &key, b"value!", None)?;
        db.delete(cf, &key, None)?;

        assert_eq!(None, db.get(cf, &key, None)?);

        Ok(())
    }

    fn delete_range_test_impl<DBT: DbLike + DeleteRange, CFT: ColumnFamilyLike>(
        db: &DBT,
        cf: &CFT,
    ) -> Result<()> {
        let mut keys = random_keys(1000);

        for key in &keys {
            db.put(cf, &key, b"foo", None)?;
        }

        keys.sort();

        assert!(keys.first().unwrap() < keys.last().unwrap());

        // Split the keys into two halves, and use those halves to produce the delete ranges
        let upper_half = keys.split_off(keys.len() / 2);
        let lower_half = keys;
        assert_lt!(lower_half.first().unwrap(), lower_half.last().unwrap());
        assert_lt!(upper_half.first().unwrap(), upper_half.last().unwrap());

        // Make a range that starts at the first key in the lower half (inclusive), and ends at the
        // first key in the upper half (exclusive).  That is, the range covers all keys in
        // `lower_half` and none in `upper_half`.
        let lower_range: KeyRange<Vec<u8>> = KeyRange::new(
            lower_half.first().unwrap().clone(),
            upper_half.first().unwrap().clone(),
        );

        // Make another range that covers the upper half.  For this one, for variety if nothing
        // else, use the `from_inclusive` helper to let us specify an inclusive upper bound that
        // gets converted into an exclusive one internally.
        let upper_range: KeyRange<Vec<u8>> = KeyRange::from_inclusive(
            upper_half.first().unwrap().clone(),
            upper_half.last().unwrap().clone(),
        );

        println!("lower_range: {}", lower_range);
        println!("upper_range: {}", upper_range);

        assert_lt!(lower_range.start(), lower_range.end());
        assert_eq!(lower_range.end(), upper_range.start());
        assert_lt!(upper_range.start(), upper_range.end());

        db.delete_range(cf, lower_range, None)?;

        // All lower half keys are gone; upper half keys are not
        for (idx, key) in lower_half.iter().enumerate() {
            assert_eq!(
                None,
                db.get(cf, &key, None)?,
                "lower_half[{}] = {}",
                idx,
                hex::encode(key.as_slice())
            );
        }

        for (idx, key) in upper_half.iter().enumerate() {
            assert_ne!(
                None,
                db.get(cf, &key, None)?,
                "upper_half[{}] = {}",
                idx,
                hex::encode(key.as_slice())
            );
        }

        db.delete_range(cf, upper_range, None)?;

        // Now ALL keys are gone
        for (idx, key) in lower_half.iter().enumerate() {
            assert_eq!(
                None,
                db.get(cf, &key, None)?,
                "lower_half[{}] = {}",
                idx,
                hex::encode(key.as_slice())
            );
        }

        for (idx, key) in upper_half.iter().enumerate() {
            assert_eq!(
                None,
                db.get(cf, &key, None)?,
                "upper_half[{}] = {}",
                idx,
                hex::encode(key.as_slice())
            );
        }

        Ok(())
    }

    fn multi_delete_range_test_impl<DBT: DbLike + DeleteRange, CFT: ColumnFamilyLike>(
        db: &DBT,
        cf: &CFT,
    ) -> Result<()> {
        let mut keys = random_keys(1000);

        for key in &keys {
            db.put(cf, &key, b"foo", None)?;
        }

        keys.sort();

        // Split the keys into two halves, and use those halves to produce the delete ranges
        let upper_half = keys.split_off(keys.len() / 2);
        let lower_half = keys;

        // This is testing the vectorized range delete, so crate several small ranges out of
        // `lower_half`, leaving `upper_half` alone.
        let ranges: Vec<KeyRange<_>> = lower_half
            .as_slice()
            .chunks(10)
            .map(|keys| {
                let start = keys.first().unwrap();
                let end = keys.last().unwrap();

                KeyRange::from_inclusive(start, end)
            })
            .collect();

        db.multi_delete_range(cf, ranges, None)?;

        // All lower half keys are gone; upper half keys are not
        for (idx, key) in lower_half.iter().enumerate() {
            assert_eq!(
                None,
                db.get(cf, &key, None)?,
                "lower_half[{}] = {}",
                idx,
                hex::encode(key.as_slice())
            );
        }

        for (idx, key) in upper_half.iter().enumerate() {
            assert_ne!(
                None,
                db.get(cf, &key, None)?,
                "upper_half[{}] = {}",
                idx,
                hex::encode(key.as_slice())
            );
        }

        Ok(())
    }

    #[test]
    fn db_delete() -> Result<()> {
        let path = TempDbPath::new();
        let db = Db::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        delete_test_impl(&db, &cf)
    }

    #[test]
    fn db_range_delete() -> Result<()> {
        let path = TempDbPath::new();
        let db = Db::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        delete_range_test_impl(&db, &cf)
    }

    #[test]
    fn db_multi_range_delete() -> Result<()> {
        let path = TempDbPath::new();
        let db = Db::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        multi_delete_range_test_impl(&db, &cf)
    }

    #[test]
    fn tx_multi_delete() -> Result<()> {
        let path = TempDbPath::new();
        let db = TransactionDb::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();
        let tx = db.begin_trans(None, None)?;

        let keys = random_keys(1000);

        for key in &keys {
            tx.put(&cf, &key, b"foo", None)?;
        }

        tx.multi_delete(&cf, &keys, None)?;

        for key in &keys {
            assert_eq!(None, db.get(&cf, &key, None)?);
        }

        Ok(())
    }

    #[test]
    fn sync_tx_multi_delete() -> Result<()> {
        let path = TempDbPath::new();
        let db = TransactionDb::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();
        let tx = db.begin_trans(None, None)?.into_sync();

        let keys = random_keys(1000);

        for key in &keys {
            tx.put(&cf, &key, b"foo", None)?;
        }

        tx.multi_delete(&cf, &keys, None)?;

        for key in &keys {
            assert_eq!(None, db.get(&cf, &key, None)?);
        }

        Ok(())
    }

    #[test]
    fn write_batch_multi_delete() -> Result<()> {
        let path = TempDbPath::new();
        let db = Db::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        let keys = random_keys(1000);

        for key in &keys {
            db.put(&cf, &key, b"foo", None)?;
        }

        let batch = WriteBatch::new()?;
        batch.multi_delete(&cf, &keys, None)?;
        db.write(batch, None)?;

        for key in &keys {
            assert_eq!(None, db.get(&cf, &key, None)?);
        }

        Ok(())
    }
}
