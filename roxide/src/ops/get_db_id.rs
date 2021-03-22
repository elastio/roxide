//! All RocksDB databases have a unique ID which is generated when they are first created and
//! cannot ever change after that.
//!
//! This isn't exposed in the C bindings but it can be obtained if one is sufficiently motivated.
use super::get_db_ptr::GetDBPtr;
use super::*;

cpp! {{
#include <rocksdb/db.h>

}}

pub trait GetDBId: RocksOpBase {
    /// Raw version of `get_db_id`, operating on unsafe C bindings.  Not intended for use by external
    /// crates.
    ///
    /// # Safety
    ///
    /// Never call this function; it's intended for internal use by the `roxide`
    /// implementation.
    unsafe fn raw_get_db_id(db_ptr: *mut libc::c_void) -> Option<String> {
        let mut db_id = String::new();
        let db_id_ptr: *mut String = &mut db_id;

        cpp!([db_ptr as "rocksdb::DB*", db_id_ptr as "void*"] {
            std::string id;
            auto res = db_ptr->GetDbIdentity(id);

            if (res == rocksdb::Status::OK()) {
                auto c_id_ptr = id.c_str();

                rust!(GetDBId_set_id_string [c_id_ptr: *const libc::c_char as "const char*", db_id_ptr: *mut String as "void*"] {
                    (*db_id_ptr).push_str(std::ffi::CStr::from_ptr(c_id_ptr).to_string_lossy().as_ref());
                });
            }
        });

        if !db_id.is_empty() {
            Some(db_id)
        } else {
            None
        }
    }

    /// Get the unique ID of this database.
    ///
    /// The ID is a string representation of a GUID.
    ///
    /// NOTE: This ID is immutable for the life of the database, *however*, when a checkpoint of
    /// the database is taken, the result is, semantically, a new database with a new ID.  Thus,
    /// this is not suitable as a way to obtain a stable ID for a database if it's possible that
    /// database will be restored from a checkpoint.
    fn get_db_id(&self) -> Option<String>;
}

impl<DB> GetDBId for DB
where
    DB: GetDBPtr,
{
    fn get_db_id(&self) -> Option<String> {
        // NOTE: This implementation is probably useless, now that we've modified the DB structs to
        // always query and cache the DB ID whenever they are opened.  I'm leaving this here for now
        // on the off chance that we find a use case for this.
        //
        // Meanwhile the most important method is `raw_get_db_id`, which is what is called from the
        // `new` method of each database impl
        let db_ptr = self.get_db_ptr();

        unsafe { Self::raw_get_db_id(db_ptr) }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::{db::*, DBLike};
    use crate::ops::DBOpen;
    use crate::test::TempDBPath;
    use crate::Result;

    #[test]
    fn db_get_id() -> Result<()> {
        let path = TempDBPath::new();
        let db = DB::open(&path, None)?;

        let id = db.get_db_id().unwrap();

        println!("DB ID: {}", id);

        assert!(!id.is_empty());

        Ok(())
    }

    #[test]
    fn id_always_different() {
        // For each created database there should be a different ID
        let path = TempDBPath::new();
        let db1 = DB::open(&path, None).unwrap();
        let id1 = db1.get_db_id().unwrap();

        let path = TempDBPath::new();
        let db2 = DB::open(&path, None).unwrap();
        let id2 = db2.get_db_id().unwrap();

        assert_ne!(id1, id2);
    }

    #[test]
    fn id_never_changes() {
        // When re-opening an existing database the ID should be unchanged
        let path = TempDBPath::new();
        let db1 = DB::open(&path, None).unwrap();
        let id1 = db1.get_db_id().unwrap();
        drop(db1);

        let db2 = DB::open(&path, None).unwrap();
        let id2 = db2.get_db_id().unwrap();

        assert_eq!(id1, id2);
    }

    #[test]
    fn id_matches_cached_id() {
        // All database structs now provide a `db_id()` method that returns a cached reference to
        // the ID string.  That should match any string returned by the `get_db_id` function
        let path = TempDBPath::new();
        let db1 = DB::open(&path, None).unwrap();
        let id1 = db1.get_db_id().unwrap();
        assert_eq!(id1, db1.db_id());
        drop(db1);

        let db2 = DB::open(&path, None).unwrap();
        assert_eq!(id1, db2.db_id());
    }

    #[test]
    fn id_is_a_uuid() {
        let path = TempDBPath::new();
        let db = DB::open(&path, None).unwrap();
        let id = db.get_db_id().unwrap();

        let uuid = uuid::Uuid::parse_str(&id).unwrap();

        assert_eq!(uuid.to_hyphenated().to_string(), id);
    }
}
