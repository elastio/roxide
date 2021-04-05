//! An operation that allows for testing for the existence of a key without retrieving its value.
//!
//! This operation is a bit faster than `Get`, because it never copies the value back into Rust.
//! While the overhead involved in that copy is minimal, it's nonetheless wasted overhead when the
//! value isn't actually needed.
use super::op_metrics;
use super::*;
use crate::db;
use crate::db_options::ReadOptions;
use crate::ffi;
use crate::handle::{RocksObject, RocksObjectDefault};
use crate::ops::{GetDbPtr, RocksOpBase};
use crate::status;
use crate::tx::{sync, unsync};
use crate::Result;
use std::ptr::NonNull;

cpp! {{
    #include "src/ops/key_exists.h"
    #include "src/ops/key_exists.cpp"
}}

// Declaration for C callable wrapper defined in `key_exists.cpp`

extern "C" {
    fn key_exists_db(
        db_ptr: *mut libc::c_void,
        options: *const ffi::rocksdb_readoptions_t,
        cf: *const ffi::rocksdb_column_family_handle_t,
        key_ptr: *const libc::c_char,
        key_len: libc::size_t,
    ) -> status::CppStatus;

    fn key_exists_tx(
        tx_ptr: *const ffi::rocksdb_transaction_t,
        options: *const ffi::rocksdb_readoptions_t,
        cf: *const ffi::rocksdb_column_family_handle_t,
        key_ptr: *const libc::c_char,
        key_len: libc::size_t,
    ) -> status::CppStatus;
}

/// Provides operations to allow the deterministic testing for the existence of a key, without
/// incurring the overhead of actually retrieving the value.
pub trait KeyExists: RocksOpBase {
    /// Raw version of `key_exists`, operating on unsafe C bindings.  Not intended for use by external
    /// crates.
    ///
    /// Not intended to be called directly.
    ///
    /// # Safety
    ///
    /// Never call this function; it's intended for internal use by the `roxide`
    /// implementation.
    unsafe fn raw_key_exists(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        key: &[u8],
        options: NonNull<ffi::rocksdb_readoptions_t>,
    ) -> Result<bool>;

    /// Tests if the key `key` exists in the column family `cf`, without actually reading the value
    /// associated with the key.
    ///
    /// This is fully deterministic, although internally it uses bloom filters as an optimization
    /// to avoid checking SST files that definitely do not contain a given key.
    fn key_exists(
        &self,
        cf: &impl db::ColumnFamilyLike,
        key: impl BinaryStr,
        options: impl Into<Option<ReadOptions>>,
    ) -> Result<bool> {
        op_metrics::instrument_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::KeyExists,
            move |reporter| {
                let options = ReadOptions::from_option(options.into());

                unsafe {
                    reporter.processing_item(key.as_slice().len(), None, || {
                        Self::raw_key_exists(
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

    /// Vectorized version of `key_exists`.
    fn keys_exist<CF: db::ColumnFamilyLike, K: BinaryStr>(
        &self,
        cf: &CF,
        keys: impl IntoIterator<Item = K>,
        options: impl Into<Option<ReadOptions>>,
    ) -> Result<Vec<(K, bool)>> {
        op_metrics::instrument_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::KeysExist,
            move |reporter| {
                let options = ReadOptions::from_option(options.into());

                let keys = keys.into_iter();
                let results = keys.map(|key| unsafe {
                    reporter.processing_item(key.as_slice().len(), None, || {
                        Self::raw_key_exists(
                            self.handle(),
                            cf.rocks_ptr(),
                            key.as_slice(),
                            options.rocks_ptr(),
                        )
                        .map(|exists| (key, exists))
                    })
                });

                // Contort ourselves a bit to pull out the per-item `Result` into one top-level `Result`
                // which will contain whatever the first error was if any
                let results: Result<Vec<_>> = results.collect();

                results
            },
        )
    }

    /// Async version of `keys_exist`
    fn async_keys_exist<CF: db::ColumnFamilyLike, K: BinaryStr + Send + 'static>(
        &self,
        cf: &CF,
        keys: impl IntoIterator<Item = K> + Send + 'static,
        options: impl Into<Option<ReadOptions>>,
    ) -> op_metrics::AsyncOpFuture<Vec<(K, bool)>>
    where
        <Self as RocksOpBase>::HandleType: Sync + Send,
    {
        op_metrics::instrument_async_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::AsyncKeysExist,
            move |reporter| {
                let handle = self.handle().clone();
                let cf = cf.clone();
                let options = ReadOptions::from_option(options.into());

                reporter.run_blocking_task(move |reporter| {
                    let keys = keys.into_iter();
                    let results = keys.map(|key| unsafe {
                        reporter.processing_item(key.as_slice().len(), None, || {
                            Self::raw_key_exists(
                                &handle,
                                cf.rocks_ptr(),
                                key.as_slice(),
                                options.rocks_ptr(),
                            )
                            .map(|exists| (key, exists))
                        })
                    });

                    // Contort ourselves a bit to pull out the per-item `Result` into one top-level `Result`
                    // which will contain whatever the first error was if any
                    let results: Result<Vec<_>> = results.collect();

                    results
                })
            },
        )
    }

    /// Scalar version of `async_keys_exist`
    fn async_key_exists_scalar<CF: db::ColumnFamilyLike, K: BinaryStr + Send + 'static>(
        &self,
        cf: &CF,
        key: K,
        options: impl Into<Option<ReadOptions>>,
    ) -> op_metrics::AsyncOpFuture<bool>
    where
        <Self as RocksOpBase>::HandleType: Sync + Send,
    {
        op_metrics::instrument_async_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::AsyncKeysExist,
            move |reporter| {
                let handle = self.handle().clone();
                let cf = cf.clone();
                let options = ReadOptions::from_option(options.into());

                reporter.run_blocking_task(move |reporter| {
                    let exists = unsafe {
                        reporter.processing_item(key.as_slice().len(), None, || {
                            {
                                Self::raw_key_exists(
                                    &handle,
                                    cf.rocks_ptr(),
                                    key.as_slice(),
                                    options.rocks_ptr(),
                                )
                            }
                        })?
                    };

                    Ok(exists)
                })
            },
        )
    }
}

impl<T: GetDbPtr> KeyExists for T {
    unsafe fn raw_key_exists(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        key: &[u8],
        options: NonNull<ffi::rocksdb_readoptions_t>,
    ) -> Result<bool> {
        let status = key_exists_db(
            Self::raw_get_db_ptr(handle),
            options.as_ptr(),
            cf.as_ptr(),
            key.as_ptr() as *const libc::c_char,
            key.len() as libc::size_t,
        )
        .into_status();

        if status.is_ok() {
            // Operation succeeded and value was found
            Ok(true)
        } else if status.code() == status::Code::NotFound {
            // Operation failed but only because the value was not found
            Ok(false)
        } else {
            // Error
            status.into_result()?;
            unreachable!()
        }
    }
}

impl KeyExists for unsync::Transaction {
    unsafe fn raw_key_exists(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        key: &[u8],
        options: NonNull<ffi::rocksdb_readoptions_t>,
    ) -> Result<bool> {
        let status = key_exists_tx(
            handle.rocks_ptr().as_ptr(),
            options.as_ptr(),
            cf.as_ptr(),
            key.as_ptr() as *const libc::c_char,
            key.len() as libc::size_t,
        )
        .into_status();

        if status.is_ok() {
            // Operation succeeded and value was found
            Ok(true)
        } else if status.code() == status::Code::NotFound {
            // Operation failed but only because the value was not found
            Ok(false)
        } else {
            // Error
            status.into_result()?;
            unreachable!()
        }
    }
}

impl KeyExists for sync::Transaction {
    unsafe fn raw_key_exists(
        _handle: &Self::HandleType,
        _cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        _key: &[u8],
        _options: NonNull<ffi::rocksdb_readoptions_t>,
    ) -> Result<bool> {
        unreachable!()
    }

    fn key_exists(
        &self,
        cf: &impl db::ColumnFamilyLike,
        key: impl BinaryStr,
        options: impl Into<Option<ReadOptions>>,
    ) -> Result<bool> {
        self.with_tx(move |tx| tx.key_exists(cf, key, options))
    }

    fn keys_exist<CF: db::ColumnFamilyLike, K: BinaryStr>(
        &self,
        cf: &CF,
        keys: impl IntoIterator<Item = K>,
        options: impl Into<Option<ReadOptions>>,
    ) -> Result<Vec<(K, bool)>> {
        self.with_tx(move |tx| tx.keys_exist(cf, keys, options))
    }

    fn async_keys_exist<CF: db::ColumnFamilyLike, K: BinaryStr + Send + 'static>(
        &self,
        cf: &CF,
        keys: impl IntoIterator<Item = K> + Send + 'static,
        options: impl Into<Option<ReadOptions>>,
    ) -> op_metrics::AsyncOpFuture<Vec<(K, bool)>>
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
            op_metrics::ColumnFamilyOperation::AsyncKeysExist,
            move |_reporter| self.async_with_tx(move |tx| tx.keys_exist(&cf_clone, keys, options)),
        )
    }
}

// The implementation for the thread-safe `Transaction` is just a wrapper around the
// `unsync::Transaction` implementation so it takes a rather different form.
#[cfg(test)]
mod test {
    use super::*;
    use crate::db::ColumnFamilyLike;
    use crate::db::DbLike;
    use crate::db::{db::*, opt_txdb::*, txdb::*};
    use crate::ops::{BeginTrans, Get, Put};
    use crate::test::{self, TempDbPath};

    fn create_test_db<DBT: DbOpen + DbLike>() -> Result<(TempDbPath, DBT, DBT::ColumnFamilyType)> {
        let path = TempDbPath::new();
        let db = DBT::create_new(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        Ok((path, db, cf))
    }

    fn key_exists_impl_test<DBT: Get + Put + KeyExists, CFT: ColumnFamilyLike>(
        db: &DBT,
        cf: &CFT,
    ) -> Result<()> {
        // Initially, the database is empty and no keys exist
        let keys = test::random_keys(100);

        for key in &keys {
            assert!(!db.key_exists(cf, key, None)?);
        }

        // Insert all of these keys into the database
        for key in &keys {
            db.put(cf, key, b"fooooooo", None)?;
        }

        // The keys exist now
        for key in &keys {
            assert!(db.key_exists(cf, key, None)?);
        }

        // And all other keys do not exist
        let more_keys = test::random_keys(100);

        for key in &more_keys {
            assert!(!db.key_exists(cf, key, None)?);
        }

        Ok(())
    }

    #[test]
    fn db_key_exists() -> Result<()> {
        let (_path, db, cf) = create_test_db::<Db>()?;

        key_exists_impl_test(&db, &cf)
    }

    #[test]
    fn txdb_key_exists() -> Result<()> {
        let (_path, db, cf) = create_test_db::<TransactionDb>()?;

        key_exists_impl_test(&db, &cf)?;
        let tx = db.begin_trans(None, None)?;
        key_exists_impl_test(&tx, &cf)
    }

    #[test]
    fn opt_txdb_key_exists() -> Result<()> {
        let (_path, db, cf) = create_test_db::<OptimisticTransactionDb>()?;

        key_exists_impl_test(&db, &cf)?;
        let tx = db.begin_trans(None, None)?;
        key_exists_impl_test(&tx, &cf)
    }

    #[test]
    fn sync_tx_key_exists() -> Result<()> {
        let (_path, db, cf) = create_test_db::<OptimisticTransactionDb>()?;
        let tx = db.begin_trans(None, None)?.into_sync();

        key_exists_impl_test(&tx, &cf)
    }
}
