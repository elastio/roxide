//! The `Merge` operation merges a new value in the database with whatever value may have existed
//! before, using a user-defined merge operator.  See the `merge` module for more.
use super::op_metrics;
use super::*;
use crate::db::{self, db::*, opt_txdb::*, txdb::*};
use crate::db_options::WriteOptions;
use crate::ffi;
use crate::handle::{RocksObject, RocksObjectDefault};
use crate::ops;
use crate::tx::{sync, unsync};
use crate::write_batch::WriteBatch;
use crate::{Error, Result};
use std::ptr::NonNull;

pub trait Merge: ops::RocksOpBase {
    /// Raw version of `merge`, operating on unsafe C bindings.  Not intended for use by external
    /// crates.
    ///
    /// Not intended to be called directly.
    ///
    /// # Safety
    ///
    /// Never call this function; it's intended for internal use by the `roxide`
    /// implementation.
    unsafe fn raw_merge(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        key: &[u8],
        value: &[u8],
        options: NonNull<ffi::rocksdb_writeoptions_t>,
    ) -> Result<()>;

    /// Merges a possibly existing value for `key` with `value` using the merge operator defined in
    /// the column family options.  Note that Rocks won't necessarily perform this merge right
    /// away; it may wait until compaction or even until `key` is read with `Get`.  It can
    /// accumulate multiple values for `key` and may merge them all at once.  This is a powerful
    /// optimization if you can be smart about how you use it.
    fn merge(
        &self,
        cf: &impl db::ColumnFamilyLike,
        key: impl BinaryStr,
        value: impl BinaryStr,
        options: impl Into<Option<WriteOptions>>,
    ) -> Result<()> {
        op_metrics::instrument_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::Merge,
            move |reporter| {
                let options = WriteOptions::from_option(options.into());

                unsafe {
                    reporter.processing_item(key.as_slice().len(), value.as_slice().len(), || {
                        Self::raw_merge(
                            self.handle(),
                            cf.rocks_ptr(),
                            key.as_slice(),
                            value.as_slice(),
                            options.rocks_ptr(),
                        )
                    })
                }
            },
        )
    }

    /// Vectorized version of `merge`
    fn multi_merge<CF: db::ColumnFamilyLike, K: BinaryStr, V: BinaryStr>(
        &self,
        cf: &CF,
        pairs: impl IntoIterator<Item = (K, V)>,
        options: impl Into<Option<WriteOptions>>,
    ) -> Result<()> {
        op_metrics::instrument_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::MultiMerge,
            move |reporter| {
                let options = WriteOptions::from_option(options.into());

                let pairs = pairs.into_iter();
                let results = pairs.map(|(key, value)| unsafe {
                    reporter.processing_item(key.as_slice().len(), value.as_slice().len(), || {
                        Self::raw_merge(
                            self.handle(),
                            cf.rocks_ptr(),
                            key.as_slice(),
                            value.as_slice(),
                            options.rocks_ptr(),
                        )
                    })
                });

                // Contort ourselves a bit to pull out the per-item `Result` into one top-level `Result`
                // which will contain whatever the first error was if any
                let results: Result<()> = results.collect();

                results
            },
        )
    }

    /// Async version of `multi_merge`
    fn async_merge<
        CF: db::ColumnFamilyLike,
        K: BinaryStr + Send + 'static,
        V: BinaryStr + Send + 'static,
    >(
        &self,
        cf: &CF,
        pairs: impl IntoIterator<Item = (K, V)> + Send + 'static,
        options: impl Into<Option<WriteOptions>>,
    ) -> op_metrics::AsyncOpFuture<()>
    where
        <Self as RocksOpBase>::HandleType: Sync + Send,
    {
        op_metrics::instrument_async_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::AsyncMerge,
            move |reporter| {
                let handle = self.handle().clone();
                let cf = cf.clone();
                let options = WriteOptions::from_option(options.into());

                reporter.run_blocking_task(move |reporter| {
                    let pairs = pairs.into_iter();
                    let results = pairs.map(|(key, value)| unsafe {
                        reporter.processing_item(
                            key.as_slice().len(),
                            value.as_slice().len(),
                            || {
                                Self::raw_merge(
                                    &handle,
                                    cf.rocks_ptr(),
                                    key.as_slice(),
                                    value.as_slice(),
                                    options.rocks_ptr(),
                                )
                            },
                        )
                    });

                    // Contort ourselves a bit to pull out the per-item `Result` into one top-level `Result`
                    // which will contain whatever the first error was if any
                    let results: Result<()> = results.collect();

                    results
                })
            },
        )
    }
}

impl Merge for Db {
    unsafe fn raw_merge(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        key: &[u8],
        value: &[u8],
        options: NonNull<ffi::rocksdb_writeoptions_t>,
    ) -> Result<()> {
        ffi_try!(ffi::rocksdb_merge_cf(
            handle.rocks_ptr().as_ptr(),
            options.as_ptr(),
            cf.as_ptr(),
            key.as_ptr() as *const libc::c_char,
            key.len() as libc::size_t,
            value.as_ptr() as *const libc::c_char,
            value.len() as libc::size_t,
        ))
    }
}

impl Merge for TransactionDb {
    unsafe fn raw_merge(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        key: &[u8],
        value: &[u8],
        options: NonNull<ffi::rocksdb_writeoptions_t>,
    ) -> Result<()> {
        ffi_try!(ffi::rocksdb_transactiondb_merge_cf(
            handle.rocks_ptr().as_ptr(),
            options.rocks_ptr().as_ptr(),
            cf.rocks_ptr().as_ptr(),
            key.as_ptr() as *const libc::c_char,
            key.len() as libc::size_t,
            value.as_ptr() as *const libc::c_char,
            value.len() as libc::size_t,
        ))
    }
}

impl Merge for OptimisticTransactionDb {
    unsafe fn raw_merge(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        key: &[u8],
        value: &[u8],
        options: NonNull<ffi::rocksdb_writeoptions_t>,
    ) -> Result<()> {
        // See comments in `Put` impl for the details of how we work with optimistic transaction
        // DBs
        Self::with_base_db(handle, |base_db| {
            ffi_try!(ffi::rocksdb_merge_cf(
                base_db,
                options.as_ptr(),
                cf.as_ptr(),
                key.as_ptr() as *const libc::c_char,
                key.len() as libc::size_t,
                value.as_ptr() as *const libc::c_char,
                value.len() as libc::size_t,
            ))
        })
    }
}

impl Merge for WriteBatch {
    unsafe fn raw_merge(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        key: &[u8],
        value: &[u8],
        _options: NonNull<ffi::rocksdb_writeoptions_t>,
    ) -> Result<()> {
        ffi::rocksdb_writebatch_merge_cf(
            handle.rocks_ptr().as_ptr(),
            cf.as_ptr(),
            key.as_ptr() as *const libc::c_char,
            key.len() as libc::size_t,
            value.as_ptr() as *const libc::c_char,
            value.len() as libc::size_t,
        );

        Ok(())
    }

    fn merge(
        &self,
        cf: &impl db::ColumnFamilyLike,
        key: impl BinaryStr,
        value: impl BinaryStr,
        options: impl Into<Option<WriteOptions>>,
    ) -> Result<()> {
        // a WriteBatch doesn't allow the caller to specify write operations, because `Merge` on a
        // write batch doesn't actually write anything.  Write options should be provided when
        // applying the write to the database via the `Write` operation.
        if options.into().is_some() {
            return Err(Error::other_error(
                "Write options for `Merge` operations on a `WriteBatch` are not supported",
            ));
        }

        unsafe {
            Self::raw_merge(
                self.handle(),
                cf.rocks_ptr(),
                key.as_slice(),
                value.as_slice(),
                WriteOptions::from_option(None).rocks_ptr(),
            )
        }
    }
}

impl Merge for unsync::Transaction {
    unsafe fn raw_merge(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        key: &[u8],
        value: &[u8],
        _options: NonNull<ffi::rocksdb_writeoptions_t>,
    ) -> Result<()> {
        ffi_try!(ffi::rocksdb_transaction_merge_cf(
            handle.rocks_ptr().as_ptr(),
            cf.as_ptr(),
            key.as_ptr() as *const libc::c_char,
            key.len() as libc::size_t,
            value.as_ptr() as *const libc::c_char,
            value.len() as libc::size_t,
        ))
    }

    fn merge(
        &self,
        cf: &impl db::ColumnFamilyLike,
        key: impl BinaryStr,
        value: impl BinaryStr,
        options: impl Into<Option<WriteOptions>>,
    ) -> Result<()> {
        // a Transaction doesn't allow the caller to specify write operations, because `Merge` on a
        // transaction doesn't actually write anything.  Write options should be provided when
        // creating the transaction with `begin_trans`
        if options.into().is_some() {
            return Err(Error::other_error(
                "Write options for `Merge` operations on a `Transaction` are not supported",
            ));
        }

        unsafe {
            Self::raw_merge(
                self.handle(),
                cf.rocks_ptr(),
                key.as_slice(),
                value.as_slice(),
                WriteOptions::from_option(None).rocks_ptr(),
            )
        }
    }
}

// The implementation for the thread-safe `Transaction` is just a wrapper around the
// `unsync::Transaction` implementation so it takes a rather different form.
impl Merge for sync::Transaction {
    unsafe fn raw_merge(
        _handle: &Self::HandleType,
        _cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        _key: &[u8],
        _value: &[u8],
        _options: NonNull<ffi::rocksdb_writeoptions_t>,
    ) -> Result<()> {
        unreachable!()
    }

    fn merge(
        &self,
        cf: &impl db::ColumnFamilyLike,
        key: impl BinaryStr,
        value: impl BinaryStr,
        options: impl Into<Option<WriteOptions>>,
    ) -> Result<()> {
        self.with_tx(move |tx| tx.merge(cf, key, value, options))
    }

    fn multi_merge<CF: db::ColumnFamilyLike, K: BinaryStr, V: BinaryStr>(
        &self,
        cf: &CF,
        pairs: impl IntoIterator<Item = (K, V)>,
        options: impl Into<Option<WriteOptions>>,
    ) -> Result<()> {
        self.with_tx(move |tx| tx.multi_merge(cf, pairs, options))
    }

    fn async_merge<
        CF: db::ColumnFamilyLike,
        K: BinaryStr + Send + 'static,
        V: BinaryStr + Send + 'static,
    >(
        &self,
        cf: &CF,
        pairs: impl IntoIterator<Item = (K, V)> + Send + 'static,
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
            op_metrics::ColumnFamilyOperation::AsyncMerge,
            move |_reporter| {
                self.async_with_tx(move |tx| tx.multi_merge(&cf_clone, pairs, options))
            },
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::*;
    use crate::db_options::DbOptions;
    use crate::merge::MergeOperands;
    use crate::ops::Get;
    use crate::test::TempDbPath;
    use crate::Result;
    #[cfg(test)]
    extern crate quickcheck;

    fn u64_from_le_bytes(bytes: &[u8]) -> u64 {
        let mut arr = [0u8; 8];

        arr.copy_from_slice(bytes);

        u64::from_le_bytes(arr)
    }

    fn u64_to_le_bytes(value: u64) -> Vec<u8> {
        let value = value.to_le_bytes();

        Vec::from(&value[..])
    }

    /// Test merge function which assumes all values are u64 integers encoded as bytes in Little
    /// Endian.  The merge function picks whatever the largest value is.
    fn test_merge(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &mut MergeOperands,
    ) -> Option<Vec<u8>> {
        // eprintln!("test_merge: existing_val={:#?} operands={:#?}", existing_val, operands.size_hint());

        let mut max = existing_val.map(u64_from_le_bytes);

        for op in operands {
            let value = u64_from_le_bytes(op);

            if max.is_none() || value > max.unwrap() {
                max = Some(value);
            }
        }

        max.map(u64_to_le_bytes)
    }

    #[quickcheck]
    fn full_merge_operator(values: Vec<u64>) -> Result<()> {
        let path = TempDbPath::new();
        let mut options = DbOptions::default();
        options.add_column_family("max");
        options.set_column_family_merge_operator(
            "max",
            "my_max_operator",
            Box::new(test_merge),
            None,
        )?;
        let db = Db::open(&path, options)?;

        let max_cf = db.get_cf("max").unwrap();

        // For every generated input, write to two keys, max and simple.  Simple is written with
        // `Put` so the last write wins.  Max has a merge operator that takes the max value only.
        for value in values.iter() {
            let value_bytes = u64_to_le_bytes(*value);
            db.merge(&max_cf, "max", &value_bytes, None)?;
            db.put(&max_cf, "simple", &value_bytes, None)?;
        }

        // Read back the keys.
        if !values.is_empty() {
            let max_value = u64_from_le_bytes(db.get(&max_cf, "max", None)?.unwrap().as_ref());
            let simple_value =
                u64_from_le_bytes(db.get(&max_cf, "simple", None)?.unwrap().as_ref());

            assert_eq!(*values.iter().max().unwrap(), max_value);
            assert_eq!(*values.as_slice().last().unwrap(), simple_value);
        } else {
            // There should be no keys
            assert_eq!(None, db.get(&max_cf, "max", None)?);
            assert_eq!(None, db.get(&max_cf, "simple", None)?);
        }

        Ok(())
    }

    /// After the merge operations before the get operations close and re-open the DB which
    /// exercises a compaction pathway
    #[quickcheck]
    fn merge_values_during_reopen(values: Vec<u64>) -> Result<()> {
        let path = TempDbPath::new();
        {
            let mut options = DbOptions::default();
            options.add_column_family("max");
            options.set_column_family_merge_operator(
                "max",
                "my_max_operator",
                Box::new(test_merge),
                None,
            )?;
            let db = Db::open(&path, options)?;

            let default_cf = db.get_cf("default").unwrap();
            let max_cf = db.get_cf("max").unwrap();

            // For every generated input, write to two keys, max and simple.  Simple is written with
            // `Put` so the last write wins.  Max has a merge operator that takes the max value only.
            for value in values.iter() {
                let value_bytes = u64_to_le_bytes(*value);
                db.merge(&max_cf, "max", &value_bytes, None)?;
                db.put(&default_cf, "simple", &value_bytes, None)?;
            }
        }

        let mut options = DbOptions::default();
        options.add_column_family("max");
        options.set_column_family_merge_operator(
            "max",
            "my_max_operator",
            Box::new(test_merge),
            None,
        )?;
        let db = Db::open(&path, options)?;
        let default_cf = db.get_cf("default").unwrap();
        let max_cf = db.get_cf("max").unwrap();

        // Read back the keys.
        if !values.is_empty() {
            let max_value = u64_from_le_bytes(db.get(&max_cf, "max", None)?.unwrap().as_ref());
            let simple_value =
                u64_from_le_bytes(db.get(&default_cf, "simple", None)?.unwrap().as_ref());

            assert_eq!(*values.iter().max().unwrap(), max_value);
            assert_eq!(*values.as_slice().last().unwrap(), simple_value);
        } else {
            // There should be no keys
            assert_eq!(None, db.get(&max_cf, "max", None)?);
            assert_eq!(None, db.get(&default_cf, "simple", None)?);
        }

        Ok(())
    }
}
