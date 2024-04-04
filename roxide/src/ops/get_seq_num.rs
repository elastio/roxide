//! Every write to a RocksDB database is assigned a unique sequence number which is a monotinically
//! increasing counter.
//!
//! This is useful to track when a particular write operation has been persisted to disk or to
//! remote long-term storage in S3.
use super::*;
use crate::ops::get_db_ptr::GetDbPtr;

pub trait GetLatestSequenceNumber: RocksOpBase {
    /// Get the sequence number of the last write operation in the database
    fn get_latest_sequence_number(&self) -> u64;
}

impl<DB> GetLatestSequenceNumber for DB
where
    DB: GetDbPtr,
{
    fn get_latest_sequence_number(&self) -> u64 {
        let db_ptr = self.get_db_ptr();
        get_latest_sequence_number_impl(db_ptr)
    }
}

pub(crate) fn get_latest_sequence_number_impl(db_ptr: *mut libc::c_void) -> u64 {
    let mut seq_num = 0u64;
    let seq_num_ptr: *mut u64 = &mut seq_num;

    unsafe {
        cpp!([db_ptr as "rocksdb::DB*", seq_num_ptr as "uint64_t*"] {
            *seq_num_ptr = db_ptr->GetLatestSequenceNumber();
        });
    }

    seq_num
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::db::*;
    use crate::db::DbLike;
    use crate::test::TempDbPath;
    use crate::Result;

    fn create_test_db() -> Result<(TempDbPath, Db, DbColumnFamily)> {
        let path = TempDbPath::new();
        let db = Db::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        Ok((path, db, cf))
    }

    #[test]
    fn seq_num_increases() -> Result<()> {
        let (_path, db, cf) = create_test_db()?;

        // No writes have taken place to the initial seq number is 0
        assert_eq!(0, db.get_latest_sequence_number());

        crate::test::fill_db(&db, &cf, 10_000)?;

        // Now it's increased
        assert_eq!(10_000, db.get_latest_sequence_number());

        Ok(())
    }
}
