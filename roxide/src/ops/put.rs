//! The `Put` operation adds or updates a value in the database
use super::op_metrics;
use super::*;
use crate::db::{self, db::*, opt_txdb::*, txdb::*};
use crate::db_options::WriteOptions;
use crate::ffi;
use crate::handle::{RocksObject, RocksObjectDefault};
use crate::tx::{sync, unsync};
use crate::write_batch::WriteBatch;
use crate::{Error, Result};
use std::ptr::NonNull;

pub trait Put: RocksOpBase {
    /// Raw version of `put`, operating on unsafe C bindings.  Not intended for use by external
    /// crates.
    ///
    /// # Safety
    ///
    /// Never call this function; it's intended for internal use by the `roxide`
    /// implementation.
    unsafe fn raw_put(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        key: &[u8],
        value: &[u8],
        options: NonNull<ffi::rocksdb_writeoptions_t>,
    ) -> Result<()>;

    /// Writes `value` to `key` in `cf`, creating `key` if it didn't exist or overwriting it if it did.
    fn put(
        &self,
        cf: &impl db::ColumnFamilyLike,
        key: impl BinaryStr,
        value: impl BinaryStr,
        options: impl Into<Option<WriteOptions>>,
    ) -> Result<()> {
        op_metrics::instrument_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::Put,
            move |reporter| {
                let options = WriteOptions::from_option(options.into());

                unsafe {
                    reporter.processing_item(key.as_slice().len(), value.as_slice().len(), || {
                        Self::raw_put(
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

    /// Vectorized version of `put`
    fn multi_put<CF: db::ColumnFamilyLike, K: BinaryStr, V: BinaryStr>(
        &self,
        cf: &CF,
        pairs: impl IntoIterator<Item = (K, V)>,
        options: impl Into<Option<WriteOptions>>,
    ) -> Result<()> {
        op_metrics::instrument_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::MultiPut,
            move |reporter| {
                let options = WriteOptions::from_option(options.into());

                let pairs = pairs.into_iter();
                let results = pairs.map(|(key, value)| unsafe {
                    reporter.processing_item(key.as_slice().len(), value.as_slice().len(), || {
                        Self::raw_put(
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

    /// Async version of `multi_put`
    fn async_put<
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
            op_metrics::ColumnFamilyOperation::AsyncPut,
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
                                Self::raw_put(
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

                    results?;

                    Ok(())
                })
            },
        )
    }

    /// Like `async_put`, but operates on a single scalar value.
    ///
    /// It's not clear if this is a good approach.  Is the async overhead too high for a single
    /// record?  More testing is needed.
    fn async_put_scalar<
        CF: db::ColumnFamilyLike,
        K: BinaryStr + Send + 'static,
        V: BinaryStr + Send + 'static,
    >(
        &self,
        cf: &CF,
        key: K,
        value: V,
        options: impl Into<Option<WriteOptions>>,
    ) -> op_metrics::AsyncOpFuture<()>
    where
        <Self as RocksOpBase>::HandleType: Sync + Send,
    {
        op_metrics::instrument_async_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::AsyncPut,
            move |reporter| {
                let handle = self.handle().clone();
                let cf = cf.clone();
                let options = WriteOptions::from_option(options.into());

                reporter.run_blocking_task(move |reporter| {
                    unsafe {
                        reporter.processing_item(
                            key.as_slice().len(),
                            value.as_slice().len(),
                            || {
                                Self::raw_put(
                                    &handle,
                                    cf.rocks_ptr(),
                                    key.as_slice(),
                                    value.as_slice(),
                                    options.rocks_ptr(),
                                )
                            },
                        )?
                    };

                    Ok(())
                })
            },
        )
    }
}

impl Put for DB {
    unsafe fn raw_put(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        key: &[u8],
        value: &[u8],
        options: NonNull<ffi::rocksdb_writeoptions_t>,
    ) -> Result<()> {
        ffi_try!(ffi::rocksdb_put_cf(
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

impl Put for TransactionDB {
    unsafe fn raw_put(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        key: &[u8],
        value: &[u8],
        options: NonNull<ffi::rocksdb_writeoptions_t>,
    ) -> Result<()> {
        ffi_try!(ffi::rocksdb_transactiondb_put_cf(
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

impl Put for OptimisticTransactionDB {
    unsafe fn raw_put(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        key: &[u8],
        value: &[u8],
        options: NonNull<ffi::rocksdb_writeoptions_t>,
    ) -> Result<()> {
        Self::with_base_db(handle, |base_db| {
            ffi_try!(ffi::rocksdb_put_cf(
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

impl Put for WriteBatch {
    unsafe fn raw_put(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        key: &[u8],
        value: &[u8],
        _options: NonNull<ffi::rocksdb_writeoptions_t>,
    ) -> Result<()> {
        ffi::rocksdb_writebatch_put_cf(
            handle.rocks_ptr().as_ptr(),
            cf.as_ptr(),
            key.as_ptr() as *const libc::c_char,
            key.len() as libc::size_t,
            value.as_ptr() as *const libc::c_char,
            value.len() as libc::size_t,
        );

        Ok(())
    }

    fn put(
        &self,
        cf: &impl db::ColumnFamilyLike,
        key: impl BinaryStr,
        value: impl BinaryStr,
        options: impl Into<Option<WriteOptions>>,
    ) -> Result<()> {
        let key = key.as_slice();
        let value = value.as_slice();

        // a WriteBatch doesn't allow the caller to specify write operations, because `Put` on a
        // write batch doesn't actually write anything.  Write options should be provided when
        // applying the write to the database via the `Write` operation.
        if options.into().is_some() {
            return Err(Error::other_error(
                "Write options for `Put` operations on a `WriteBatch` are not supported",
            ));
        }

        let options = WriteOptions::from_option(None);

        unsafe {
            Self::raw_put(
                self.handle(),
                cf.rocks_ptr(),
                key.as_slice(),
                value.as_slice(),
                options.rocks_ptr(),
            )
        }
    }
}

impl Put for unsync::Transaction {
    unsafe fn raw_put(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        key: &[u8],
        value: &[u8],
        _options: NonNull<ffi::rocksdb_writeoptions_t>,
    ) -> Result<()> {
        ffi_try!(ffi::rocksdb_transaction_put_cf(
            handle.rocks_ptr().as_ptr(),
            cf.as_ptr(),
            key.as_ptr() as *const libc::c_char,
            key.len() as libc::size_t,
            value.as_ptr() as *const libc::c_char,
            value.len() as libc::size_t,
        ))
    }

    fn put(
        &self,
        cf: &impl db::ColumnFamilyLike,
        key: impl BinaryStr,
        value: impl BinaryStr,
        options: impl Into<Option<WriteOptions>>,
    ) -> Result<()> {
        // a transaction doesn't allow the caller to specify write operations, because `Put` on a
        // write batch doesn't actually write anything.  Write options should be provided when
        // applying the write to the database via the `Write` operation.
        if options.into().is_some() {
            return Err(Error::other_error(
                "Write options for `Put` operations on a `Transaction` are not supported",
            ));
        }

        let options = WriteOptions::from_option(None);

        unsafe {
            Self::raw_put(
                self.handle(),
                cf.rocks_ptr(),
                key.as_slice(),
                value.as_slice(),
                options.rocks_ptr(),
            )
        }
    }
}

// The implementation for the thread-safe `Transaction` is just a wrapper around the
// `unsync::Transaction` implementation so it takes a rather different form.
impl Put for sync::Transaction {
    unsafe fn raw_put(
        _handle: &Self::HandleType,
        _cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        _key: &[u8],
        _value: &[u8],
        _options: NonNull<ffi::rocksdb_writeoptions_t>,
    ) -> Result<()> {
        unreachable!()
    }

    fn put(
        &self,
        cf: &impl db::ColumnFamilyLike,
        key: impl BinaryStr,
        value: impl BinaryStr,
        options: impl Into<Option<WriteOptions>>,
    ) -> Result<()> {
        self.with_tx(move |tx| tx.put(cf, key, value, options))
    }

    /// Vectorized version of `put`
    fn multi_put<CF: db::ColumnFamilyLike, K: BinaryStr, V: BinaryStr>(
        &self,
        cf: &CF,
        pairs: impl IntoIterator<Item = (K, V)>,
        options: impl Into<Option<WriteOptions>>,
    ) -> Result<()> {
        self.with_tx(move |tx| tx.multi_put(cf, pairs, options))
    }

    /// Async version of `put`
    fn async_put<
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
            op_metrics::ColumnFamilyOperation::AsyncPut,
            move |_reporter| self.async_with_tx(move |tx| tx.multi_put(&cf_clone, pairs, options)),
        )
    }

    /// Async version of `put`
    fn async_put_scalar<
        CF: db::ColumnFamilyLike,
        K: BinaryStr + Send + 'static,
        V: BinaryStr + Send + 'static,
    >(
        &self,
        cf: &CF,
        key: K,
        value: V,
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
            op_metrics::ColumnFamilyOperation::AsyncPut,
            move |_reporter| self.async_with_tx(move |tx| tx.put(&cf_clone, key, value, options)),
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::DBLike;
    use crate::ops::begin_tx::BeginTrans;
    use crate::test::TempDBPath;

    #[test]
    fn db_simple_put() -> Result<()> {
        let path = TempDBPath::new();
        let db = DB::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        db.put(&cf, "foo", "bar", None)?;
        db.put(&cf, "baz", "boo", None)?;

        Ok(())
    }

    #[test]
    fn txdb_simple_put() -> Result<()> {
        let path = TempDBPath::new();
        let db = TransactionDB::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        db.put(&cf, "foo", "bar", None)?;
        db.put(&cf, "baz", "boo", None)?;

        Ok(())
    }

    #[test]
    fn opt_txdb_simple_put() -> Result<()> {
        let path = TempDBPath::new();
        let db = OptimisticTransactionDB::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        db.put(&cf, "foo", "bar", None)?;
        db.put(&cf, "baz", "boo", None)?;

        Ok(())
    }

    #[test]
    fn batch_simple_put() -> Result<()> {
        let path = TempDBPath::new();
        let db = DB::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        let batch = WriteBatch::new()?;
        batch.put(&cf, "foo", "bar", None)?;
        batch.put(&cf, "baz", "boo", None)?;

        Ok(())
    }

    #[test]
    fn tx_simple_put() -> Result<()> {
        let path = TempDBPath::new();
        let db = TransactionDB::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        let tx = db.begin_trans(None, None)?;
        tx.put(&cf, "foo", "bar", None)?;
        tx.put(&cf, "baz", "boo", None)?;

        tx.commit()?;

        Ok(())
    }

    #[test]
    fn sync_tx_simple_put() -> Result<()> {
        let path = TempDBPath::new();
        let db = TransactionDB::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        let tx = db.begin_trans(None, None)?.into_sync();
        tx.put(&cf, "foo", "bar", None)?;
        tx.put(&cf, "baz", "boo", None)?;

        tx.commit()?;

        Ok(())
    }
}
