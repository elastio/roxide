//! This module implements `DB`, the standard RocksDB database type which is appropriate for most
//! use cases.
use super::*;
use crate::rocks_class;

rocks_class!(DBHandle, ffi::rocksdb_t, ffi::rocksdb_close, @send, @sync);
rocks_db_impl!(DB, DBColumnFamily, DBHandle, ffi::rocksdb_t);

impl DB {
    /// Destroys the database at the specified path, including any files used by that database that
    /// happen to be in another directory
    pub fn destroy<O: Into<Option<DBOptions>>, P: AsRef<Path>>(
        db_options: O,
        path: P,
    ) -> Result<()> {
        let db_options = db_options.into().unwrap_or_default();
        let (options, _) = db_options.into_components()?;
        let cpath = path_to_cstring(path)?;
        unsafe {
            ffi_try!(ffi::rocksdb_destroy_db(options.inner, cpath.as_ptr(),))?;
        }
        Ok(())
    }

    pub(crate) fn ffi_open(
        options: *const ffi::rocksdb_options_t,
        path: *const libc::c_char,
        num_column_families: libc::c_int,
        column_family_names: *mut *const libc::c_char,
        column_family_options: *mut *const ffi::rocksdb_options_t,
        column_family_handles: *mut *mut ffi::rocksdb_column_family_handle_t,
    ) -> Result<*mut ffi::rocksdb_t> {
        unsafe {
            ffi_try!(ffi::rocksdb_open_column_families(
                options,
                path,
                num_column_families,
                column_family_names,
                column_family_options,
                column_family_handles,
            ))
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::ops::DBOpen;
    use crate::test::TempDBPath;

    #[test]
    fn dangling_column_family() -> Result<()> {
        // Make a database with a few column families.  Then drop everything but one remaining
        // column family handle.  It should still work.
        let path = TempDBPath::new();
        let mut options = DBOptions::default();
        options.add_column_family("foo");
        options.add_column_family("bar");

        let db = DB::open(&path, options)?;

        let foo = db.get_cf("foo").unwrap();
        let bar = db.get_cf("bar").unwrap();

        drop(db);
        drop(foo);

        // TODO: Once there are operations implemented, do Puts and Gets here
        drop(bar);

        Ok(())
    }

    #[test]
    fn cf_name_correct() -> Result<()> {
        let path = TempDBPath::new();
        let mut options = DBOptions::default();
        options.add_column_family("foo");
        options.add_column_family("bar");

        let db = DB::open(&path, options)?;

        let foo = db.get_cf("foo").unwrap();
        assert_eq!("foo", foo.name());

        let bar = db.get_cf("bar").unwrap();
        assert_eq!("bar", bar.name());

        Ok(())
    }

    #[test]
    fn no_extra_cfs() -> Result<()> {
        let path = TempDBPath::new();
        let mut options = DBOptions::default();
        options.add_column_family("bar");

        let db = DB::open(&path, options)?;

        assert_eq!(vec!["default", "bar"].sort(), db.get_cf_names().sort());

        Ok(())
    }

    #[test]
    #[should_panic]
    fn cant_open_more_than_once() {
        // It's not possible to open the same database twice; each open is exclusive
        let path = TempDBPath::new();
        let _db1 = DB::create_new(&path, None).unwrap();

        DB::open_existing(&path, None).unwrap();
    }

    #[test]
    fn closes_on_drop() -> Result<()> {
        // Once the database is dropped, the database is closed and another instance can be created
        // It's not possible to open the same database twice; each open is exclusive
        let path = TempDBPath::new();
        let db1 = DB::create_new(&path, None)?;

        drop(db1);

        DB::open_existing(&path, None)?;

        Ok(())
    }

    #[test]
    #[should_panic]
    fn cant_open_with_cf_alive() {
        // Even if the only thing left is a column family object, the db is still locked
        // It's not possible to open the same database twice; each open is exclusive
        let path = TempDBPath::new();
        let db1 = DB::create_new(&path, None).unwrap();
        let _cf = db1.get_cf("default").unwrap();

        drop(db1);

        DB::open_existing(&path, None).unwrap();
    }

    #[test]
    fn closes_on_cf_drop() -> Result<()> {
        // Dropping the last CF also closes the database
        let path = TempDBPath::new();
        let db1 = DB::create_new(&path, None).unwrap();
        let cf = db1.get_cf("default").unwrap();

        drop(db1);
        drop(cf);

        DB::open_existing(&path, None)?;

        Ok(())
    }

    #[test]
    fn gets_cf_names() -> Result<()> {
        let path = TempDBPath::new();
        let mut options = DBOptions::default();
        options.add_column_family("foo");
        options.add_column_family("bar");

        let db = DB::open(&path, options)?;

        let cf_names = db.get_cf_names();

        assert!(cf_names.contains(&"default")); // All DBs have 'default' automatically
        assert!(cf_names.contains(&"foo"));
        assert!(cf_names.contains(&"bar"));
        Ok(())
    }

    #[test]
    fn setup_database_loggers_works() -> Result<()> {
        let path = TempDBPath::new();
        let logger = crate::logging::tests::TestLogger::new();
        let context = logger.context.clone();
        let mut options = DBOptions::default();
        options.set_logger(log::LevelFilter::Debug, logger);

        let db = DB::open(&path, options)?;
        // crate::db::setup_database_loggers(&db);
        drop(db);

        // Verify the logger's context was set
        let db_path: &Path = path.path();
        let context_hashmap = context.lock().unwrap();

        println!("Context hash: {:#?}", context_hashmap);
        assert_eq!(
            context_hashmap.get("db_path").unwrap(),
            &db_path.to_string_lossy()
        );

        Ok(())
    }

    #[test]
    fn setup_database_loggers_works_without_logger() -> Result<()> {
        let path = TempDBPath::new();
        let options = DBOptions::default();

        let db = DB::open(&path, options)?;
        drop(db);

        Ok(())
    }
}
