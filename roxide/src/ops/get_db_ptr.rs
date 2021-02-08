//! The `GetDBPtr` operation exposes the raw C++ pointer `rocksdb::DB` base class of
//! any RocksDB database type.  That struct contains some state, most importantly `Statistics`,
//! that is of interest to Rust code
use super::*;
use crate::db::{db::*, opt_txdb::*, txdb::*};
use crate::handle::RocksObject;

cpp! {{
    #include "src/lib.h"
}}

pub trait GetDBPtr: RocksOpBase {
    /// Raw version of `get_db_ptr`, operating on unsafe C bindings.  Not intended for use by external
    /// crates.
    ///
    /// # Safety
    ///
    /// Never call this function; it's intended for internal use by the `roxide`
    /// implementation.
    unsafe fn raw_get_db_ptr(handle: &Self::HandleType) -> *mut libc::c_void;

    fn get_db_ptr(&self) -> *mut libc::c_void {
        unsafe { Self::raw_get_db_ptr(self.handle()) }
    }
}

impl GetDBPtr for DB {
    unsafe fn raw_get_db_ptr(handle: &Self::HandleType) -> *mut libc::c_void {
        let db_ptr = handle.rocks_ptr().as_ptr();

        cpp!([db_ptr as "::rocksdb_t*"] -> *mut libc::c_void as "rocksdb::DB*" {
            return cast_to_db(db_ptr);
        })
    }
}

impl GetDBPtr for TransactionDB {
    unsafe fn raw_get_db_ptr(handle: &Self::HandleType) -> *mut libc::c_void {
        let db_ptr = handle.rocks_ptr().as_ptr();

        cpp!([db_ptr as "::rocksdb_transactiondb_t*"] -> *mut libc::c_void as "rocksdb::DB*" {
            return cast_to_db(db_ptr);
        })
    }
}

impl GetDBPtr for OptimisticTransactionDB {
    unsafe fn raw_get_db_ptr(handle: &Self::HandleType) -> *mut libc::c_void {
        let db_ptr = handle.rocks_ptr().as_ptr();

        cpp!([db_ptr as "::rocksdb_optimistictransactiondb_t*"] -> *mut libc::c_void as "rocksdb::DB*" {
            return cast_to_db(db_ptr);
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::ops::DBOpen;
    use crate::test::TempDBPath;
    use crate::Result;

    #[test]
    fn db_get_ptr() -> Result<()> {
        let path = TempDBPath::new();
        let db = DB::open(&path, None)?;

        let db_ptr = db.get_db_ptr();

        validate_db_ptr(db_ptr);

        Ok(())
    }

    #[test]
    fn txdb_get_ptr() -> Result<()> {
        let path = TempDBPath::new();
        let db = TransactionDB::open(&path, None)?;

        let db_ptr = db.get_db_ptr();

        validate_db_ptr(db_ptr);

        Ok(())
    }

    #[test]
    fn opt_txdb_get_ptr() -> Result<()> {
        let path = TempDBPath::new();
        let db = OptimisticTransactionDB::open(&path, None)?;

        let db_ptr = db.get_db_ptr();

        validate_db_ptr(db_ptr);

        Ok(())
    }

    fn validate_db_ptr(db_ptr: *mut libc::c_void) {
        // Does something with the pointer that will crash or fail if there's something wrong with
        // the code
        assert!(!db_ptr.is_null());

        unsafe {
            let err_msg = cpp!([db_ptr as "rocksdb::DB*"] -> *const libc::c_char as "const char*" {
                std::string prop_value;
                auto result = db_ptr->GetProperty(rocksdb::DB::Properties::kStats, &prop_value);

                if (!result) {
                    return "GetProperty failed";
                } else if (prop_value.size() == 0) {
                    return "kStats property is empty";
                } else {
                    return NULL;
                }
            });

            if !err_msg.is_null() {
                let message = std::ffi::CStr::from_ptr(err_msg)
                    .to_string_lossy()
                    .into_owned();
                panic!("{}", message);
            }
        }
    }
}
