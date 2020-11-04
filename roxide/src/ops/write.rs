//! The `Write` operation (not to be confused with `Put`) writes a `WriteBatch` batch of previously
//! prepared writes, all at once.  This batch is either all written successfully, or the write
//! fails and nothing in the batch is written.  RocksDB guarantees that no other threads will see
//! the writes until the batch completes, at which point other threads will see all of the writes.
use super::op_metrics;
use super::*;
use crate::db::{db::*, opt_txdb::*, txdb::*};
use crate::db_options::WriteOptions;
use crate::ffi;
use crate::handle::{RocksObject, RocksObjectDefault};
use crate::write_batch::WriteBatch;
use crate::Result;

pub trait Write: RocksOpBase {
    /// Writes a previously prepared write batch to the database
    fn write(&self, batch: WriteBatch, options: impl Into<Option<WriteOptions>>) -> Result<()>;
}

impl Write for DB {
    fn write(&self, batch: WriteBatch, options: impl Into<Option<WriteOptions>>) -> Result<()> {
        op_metrics::instrument_db_op(self, op_metrics::DatabaseOperation::Write, move || {
            let options = WriteOptions::from_option(options.into());

            unsafe {
                ffi_try!(ffi::rocksdb_write(
                    self.rocks_ptr().as_ptr(),
                    options.rocks_ptr().as_ptr(),
                    batch.rocks_ptr().as_ptr(),
                ))
            }
        })
    }
}

impl Write for TransactionDB {
    fn write(&self, batch: WriteBatch, options: impl Into<Option<WriteOptions>>) -> Result<()> {
        op_metrics::instrument_db_op(self, op_metrics::DatabaseOperation::Write, move || {
            let options = WriteOptions::from_option(options.into());

            unsafe {
                ffi_try!(ffi::rocksdb_transactiondb_write(
                    self.rocks_ptr().as_ptr(),
                    options.rocks_ptr().as_ptr(),
                    batch.rocks_ptr().as_ptr(),
                ))
            }
        })
    }
}

impl Write for OptimisticTransactionDB {
    fn write(&self, batch: WriteBatch, options: impl Into<Option<WriteOptions>>) -> Result<()> {
        op_metrics::instrument_db_op(self, op_metrics::DatabaseOperation::Write, move || {
            let options = WriteOptions::from_option(options.into());

            // The optimistic transaction DB API is slightly different than either of the others.
            // See the comment in `put.rs` for the details
            unsafe {
                Self::with_base_db(self.handle(), |base_db| {
                    ffi_try!(ffi::rocksdb_write(
                        base_db,
                        options.rocks_ptr().as_ptr(),
                        batch.rocks_ptr().as_ptr(),
                    ))
                })
            }
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::DBLike;
    use crate::ops::{DBOpen, Put};
    use crate::test::TempDBPath;

    #[test]
    fn db_write() -> Result<()> {
        let path = TempDBPath::new();
        let db = DB::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        let batch = WriteBatch::new()?;
        batch.put(&cf, "foo", "bar", None)?;
        batch.put(&cf, "baz", "boo", None)?;

        db.write(batch, None)?;

        Ok(())
    }

    #[test]
    fn txdb_simple_put() -> Result<()> {
        let path = TempDBPath::new();
        let db = TransactionDB::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        let batch = WriteBatch::new()?;
        batch.put(&cf, "foo", "bar", None)?;
        batch.put(&cf, "baz", "boo", None)?;

        db.write(batch, None)?;

        Ok(())
    }

    #[test]
    fn opt_txdb_simple_put() -> Result<()> {
        let path = TempDBPath::new();
        let db = OptimisticTransactionDB::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        let batch = WriteBatch::new()?;
        batch.put(&cf, "foo", "bar", None)?;
        batch.put(&cf, "baz", "boo", None)?;

        db.write(batch, None)?;

        Ok(())
    }
}
