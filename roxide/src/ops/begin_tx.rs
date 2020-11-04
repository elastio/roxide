//! Starts a new transaction, for database implementations that support transactions.

use super::op_metrics;
use crate::db::DBLike;
use crate::db::{
    opt_txdb::{OptimisticTransactionDB, OptimisticTransactionOptions},
    txdb::{TransactionDB, TransactionOptions},
};
use crate::db_options::WriteOptions;
use crate::ffi;
use crate::handle::{RocksObject, RocksObjectDefault};
use crate::ops::RocksOpBase;
use crate::tx::{sync, unsync};
use crate::{Error, Result};
use std::ptr;
use std::ptr::NonNull;

/// Provides the `begin_trans` operation to start an isolated transaction on the database.
///
/// Note that all databases supporting `BeginTrans` must be `Send` and `Sync` so that multiple
/// transactions can be performed on separate threads
pub trait BeginTrans: RocksOpBase + DBLike {
    /// The type of the native FFI struct that the RocksDB C API expectes for the transaction
    /// options
    type TransactionOptionsNativeType;

    /// The type of the Rust options struct that holds the transaction options
    type TransactionOptionsType: RocksObjectDefault<Self::TransactionOptionsNativeType>;

    /// Raw version of `begin_trans`, operating on unsafe C bindings.  Not intended for use by external
    /// crates.
    ///
    ///
    /// # Safety
    ///
    /// Never call this function; it's intended for internal use by the `roxide`
    /// implementation.
    unsafe fn raw_begin_trans(
        &self,
        write_options: NonNull<ffi::rocksdb_writeoptions_t>,
        tx_options: NonNull<Self::TransactionOptionsNativeType>,
    ) -> Result<unsync::Transaction>;

    /// Starts a new transaction, with is isolated from changes made in the underlying database and
    /// other transactions.
    ///
    /// Note that, unlike database types, transactions are not `Sync` so they cannot be used from
    /// multiple threads simultaneously.
    fn begin_trans(
        &self,
        write_options: impl Into<Option<WriteOptions>>,
        tx_options: impl Into<Option<Self::TransactionOptionsType>>,
    ) -> Result<unsync::Transaction> {
        op_metrics::instrument_db_op(self, op_metrics::DatabaseOperation::BeginTrans, move || {
            let write_options = WriteOptions::from_option(write_options.into());
            let tx_options = Self::TransactionOptionsType::from_option(tx_options.into());

            unsafe { self.raw_begin_trans(write_options.rocks_ptr(), tx_options.rocks_ptr()) }
        })
    }

    /// Asynchronously creates a new transaction in a worker thread.
    ///
    /// # Notes
    ///
    /// Because this operation happens in another thread, and in general is used for async
    /// operations, this version produces a `sync::Transaction`, which is the thread-safe wrapper
    /// around `unsync::Transaction`.
    fn async_begin_trans(
        &self,
        write_options: impl Into<Option<WriteOptions>>,
        tx_options: impl Into<Option<Self::TransactionOptionsType>>,
    ) -> op_metrics::AsyncOpFuture<sync::Transaction>
    where
        <Self as RocksOpBase>::HandleType: Sync + Send,
        Self::TransactionOptionsType: Send + 'static,
        Self::TransactionOptionsNativeType: 'static,
        Self: Sync + Clone + 'static,
    {
        op_metrics::instrument_async_db_op(
            self,
            op_metrics::DatabaseOperation::AsyncBeginTrans,
            move |labels| {
                let me = (*self).clone();
                let write_options = WriteOptions::from_option(write_options.into());
                let tx_options = Self::TransactionOptionsType::from_option(tx_options.into());

                op_metrics::run_blocking_db_task(&labels, move || {
                    let tx = unsafe {
                        me.raw_begin_trans(write_options.rocks_ptr(), tx_options.rocks_ptr())?
                    };

                    Ok(tx.into_sync())
                })
            },
        )
    }
}

impl BeginTrans for TransactionDB {
    type TransactionOptionsNativeType = ffi::rocksdb_transaction_options_t;
    type TransactionOptionsType = TransactionOptions;

    unsafe fn raw_begin_trans(
        &self,
        write_options: NonNull<ffi::rocksdb_writeoptions_t>,
        tx_options: NonNull<Self::TransactionOptionsNativeType>,
    ) -> Result<unsync::Transaction> {
        let handle = self.handle();
        let tx_ptr = ffi::rocksdb_transaction_begin(
            handle.as_ptr(),
            write_options.as_ptr(),
            tx_options.as_ptr(),
            ptr::null_mut(),
        );

        if let Some(tx_ptr) = ptr::NonNull::new(tx_ptr) {
            Ok(unsync::Transaction::new(self, tx_ptr))
        } else {
            Err(Error::other_error("begin transaction failed"))
        }
    }
}

impl BeginTrans for OptimisticTransactionDB {
    type TransactionOptionsNativeType = ffi::rocksdb_optimistictransaction_options_t;
    type TransactionOptionsType = OptimisticTransactionOptions;

    unsafe fn raw_begin_trans(
        &self,
        write_options: NonNull<ffi::rocksdb_writeoptions_t>,
        tx_options: NonNull<Self::TransactionOptionsNativeType>,
    ) -> Result<unsync::Transaction> {
        let handle = self.handle();
        let tx_ptr = ffi::rocksdb_optimistictransaction_begin(
            handle.as_ptr(),
            write_options.as_ptr(),
            tx_options.as_ptr(),
            ptr::null_mut(),
        );

        if let Some(tx_ptr) = ptr::NonNull::new(tx_ptr) {
            Ok(unsync::Transaction::new(self, tx_ptr))
        } else {
            Err(Error::other_error("begin transaction failed"))
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::DBLike;
    use crate::ops::{DBOpen, Get};
    use crate::test;
    use crate::test::TempDBPath;

    #[test]
    fn begin_trans() -> Result<()> {
        let path = TempDBPath::new();
        let db = TransactionDB::open(&path, None)?;
        let _cf = db.get_cf("default").unwrap();

        let _trans = db.begin_trans(None, None)?;

        Ok(())
    }

    #[test]
    fn opt_txdb_begin_trans() -> Result<()> {
        let path = TempDBPath::new();
        let db = OptimisticTransactionDB::open(&path, None)?;
        let _cf = db.get_cf("default").unwrap();

        let _trans = db.begin_trans(None, None)?;

        Ok(())
    }

    #[test]
    fn db_async_begin_trans() -> Result<()> {
        let path = TempDBPath::new();
        let db = TransactionDB::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        let mut rt = elasyncio::init_tokio_runtime()?;

        rt.block_on(async move {
            let tx = db.async_begin_trans(None, None).await?;
            test::fill_db_deterministic(&tx, &cf, 42, 1_000)?;

            let keys = test::random_keys_deterministic(42, 1_000);

            let pairs = tx.async_get(&cf, keys.clone(), None).await?;

            assert_eq!(
                keys,
                pairs
                    .into_iter()
                    .map(|(key, _value)| key)
                    .collect::<Vec<Vec<u8>>>()
            );

            tx.async_commit().await?;

            Ok(())
        })
    }
}
