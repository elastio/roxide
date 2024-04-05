//! This module implements `DB`, the standard RocksDB database type which is appropriate for most
//! use cases.
use super::*;
use crate::rocks_class;

rocks_class!(DbHandle, ffi::rocksdb_t, ffi::rocksdb_close, @send, @sync);
rocks_db_impl!(Db, DbColumnFamily, DbHandle, ffi::rocksdb_t);

impl Db {
    /// Destroys the database at the specified path, including any files used by that database that
    /// happen to be in another directory
    pub fn destroy<O: Into<Option<DbOptions>>, P: AsRef<Path>>(
        db_options: O,
        path: P,
    ) -> Result<()> {
        let db_options = db_options.into().unwrap_or_default();
        let components = db_options.into_components()?;
        let cpath = path_to_cstring(path)?;
        unsafe {
            ffi_try!(ffi::rocksdb_destroy_db(
                components.options.inner,
                cpath.as_ptr(),
            ))?;
        }
        Ok(())
    }
}

// There are no errors that need to be post-processed for this DB type
impl crate::error::ErrorPostprocessor for Db {}

#[cfg(test)]

mod test {
    use super::*;
    use crate::ops::{Compact, DbOpen};
    use crate::test::TempDbPath;

    #[test]
    fn dangling_column_family() -> Result<()> {
        // Make a database with a few column families.  Then drop everything but one remaining
        // column family handle.  It should still work.
        let path = TempDbPath::new();
        let mut options = DbOptions::default();
        options.add_column_family("boo");
        options.add_column_family("bar");

        let db = Db::open(&path, options)?;

        let boo = db.get_cf("boo").unwrap();
        let bar = db.get_cf("bar").unwrap();

        drop(db);
        drop(boo);

        // TODO: Once there are operations implemented, do Puts and Gets here
        drop(bar);

        Ok(())
    }

    #[test]
    fn cf_name_correct() -> Result<()> {
        let path = TempDbPath::new();
        let mut options = DbOptions::default();
        options.add_column_family("boo");
        options.add_column_family("bar");

        let db = Db::open(&path, options)?;

        let boo = db.get_cf("boo").unwrap();
        assert_eq!("boo", boo.name().as_str());

        let bar = db.get_cf("bar").unwrap();
        assert_eq!("bar", bar.name().as_str());

        Ok(())
    }

    #[test]
    fn no_extra_cfs() -> Result<()> {
        let path = TempDbPath::new();
        let mut options = DbOptions::default();
        options.add_column_family("bar");

        let db = Db::open(&path, options)?;

        assert_eq!(
            &["default", "bar"].sort_unstable(),
            &db.get_cf_names().sort_unstable()
        );

        Ok(())
    }

    #[test]
    #[should_panic]
    fn cant_open_more_than_once() {
        // It's not possible to open the same database twice; each open is exclusive
        let path = TempDbPath::new();
        let _db1 = Db::create_new(&path, None).unwrap();

        Db::open_existing(&path, None).unwrap();
    }

    #[test]
    fn closes_on_drop() -> Result<()> {
        // Once the database is dropped, the database is closed and another instance can be created
        // It's not possible to open the same database twice; each open is exclusive
        let path = TempDbPath::new();
        let db1 = Db::create_new(&path, None)?;

        drop(db1);

        Db::open_existing(&path, None)?;

        Ok(())
    }

    #[test]
    #[should_panic]
    fn cant_open_with_cf_alive() {
        // Even if the only thing left is a column family object, the db is still locked
        // It's not possible to open the same database twice; each open is exclusive
        let path = TempDbPath::new();
        let db1 = Db::create_new(&path, None).unwrap();
        let _cf = db1.get_cf("default").unwrap();

        drop(db1);

        Db::open_existing(&path, None).unwrap();
    }

    #[test]
    fn closes_on_cf_drop() -> Result<()> {
        // Dropping the last CF also closes the database
        let path = TempDbPath::new();
        let db1 = Db::create_new(&path, None).unwrap();
        let cf = db1.get_cf("default").unwrap();

        drop(db1);
        drop(cf);

        Db::open_existing(&path, None)?;

        Ok(())
    }

    #[test]
    fn gets_cf_names() -> Result<()> {
        let path = TempDbPath::new();
        let mut options = DbOptions::default();
        options.add_column_family("boo");
        options.add_column_family("bar");

        let db = Db::open(&path, options)?;

        let cf_names = db.get_cf_names();

        assert!(cf_names.contains(&"default")); // All DBs have 'default' automatically
        assert!(cf_names.contains(&"boo"));
        assert!(cf_names.contains(&"bar"));
        Ok(())
    }

    #[test]
    fn setup_database_loggers_works() -> Result<()> {
        let path = TempDbPath::new();
        let logger = crate::logging::tests::TestLogger::new();
        let context = logger.context.clone();
        let mut options = DbOptions::default();
        options.set_logger(log::LevelFilter::Debug, logger);

        let db = Db::open(&path, options)?;
        // crate::Db::setup_database_loggers(&db);
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
        let path = TempDbPath::new();
        let options = DbOptions::default();

        let db = Db::open(&path, options)?;
        drop(db);

        Ok(())
    }

    /// When we set a db_path in DBOptions, that overrides the path specified in open or create
    #[test]
    fn db_path_overrides_open() -> Result<()> {
        let path = TempDbPath::new();
        let db_path = path.path().join("custom_path");
        std::fs::create_dir_all(&db_path).unwrap();
        let mut options = DbOptions::default();
        options.add_column_family("boo");
        options.add_column_family("bar");

        // Override the database path with this explicit path
        options.set_db_path(&db_path);

        // Create the database and put some data in it then close it
        {
            let db = Db::create_new(&path, options)?;

            // Put something in the DB so data files will have to be written
            //
            // Note compaction is required here otherwise they might not be flushed to SSTs
            let cf = db.get_cf("boo").unwrap();
            crate::test::fill_db(&db, &cf, 10_000)?;
            db.compact_all(&cf, None)?;

            let cf = db.get_cf("bar").unwrap();
            crate::test::fill_db(&db, &cf, 10_000)?;
            db.compact_all(&cf, None)?;
        }

        // Even though we set the db path, non-data files like manifest, options, and WAL
        // are written tot he path that was passed to `create_new`
        assert_dir_contains_files(path.path(), true);

        // Because we generated some writes to the CFs and triggered compaction, there are
        // files in the custom DB path also
        assert_dir_contains_files(&db_path, true);

        Ok(())
    }

    /// When we set a custom path for a ColumnFamily, that overrides any custom DB path or path
    /// specified in open or create
    #[test]
    fn cf_path_overrides_open() -> Result<()> {
        let path = TempDbPath::new();
        let db_path = path.path().join("custom_path");
        let boo_path = db_path.join("boo");
        let bar_path = db_path.join("bar");
        std::fs::create_dir_all(&db_path).unwrap();
        std::fs::create_dir_all(&boo_path).unwrap();
        std::fs::create_dir_all(&bar_path).unwrap();
        let mut options = DbOptions::default();
        options.add_column_family("boo");
        options.add_column_family("bar");
        options.set_db_path(&db_path);
        options.set_column_family_path("boo", &boo_path);
        options.set_column_family_path("bar", &bar_path);

        // Create the database and put some data in it then close it
        {
            let db = Db::create_new(&path, options)?;

            // Put something in the DB so data files will have to be written
            //
            // Note compaction is required here otherwise they might not be flushed to SSTs
            let cf = db.get_cf("boo").unwrap();
            crate::test::fill_db(&db, &cf, 10_000)?;
            db.compact_all(&cf, None)?;

            let cf = db.get_cf("bar").unwrap();
            crate::test::fill_db(&db, &cf, 10_000)?;
            db.compact_all(&cf, None)?;
        }

        // Even though we set the db path, non-data files like manifest, options, and WAL
        // are written tot he path that was passed to `create_new`
        assert_dir_contains_files(path.path(), true);

        // Because we generated some writes to CF `boo` and triggered a compaction, there are
        // files in `boo`s custom path also
        assert_dir_contains_files(&boo_path, true);

        // For the same reason, files in `bar`'s custom path
        assert_dir_contains_files(&bar_path, true);

        // Since both CFs had their paths overridden, the custom path for the DB should be
        // empty, since there's nothing to write there
        assert_dir_contains_files(&db_path, false);

        Ok(())
    }

    fn assert_dir_contains_files(dir: &Path, contains: bool) {
        use walkdir::WalkDir;

        assert!(dir.exists(), "Directory {} doesn't exist", dir.display());

        let mut has_file = false;

        // Print the entire tree for debugging purposes.
        //
        // The directory is only said to contain files if it directly contains them; files in
        // subdirectories don't count
        for entry in WalkDir::new(dir) {
            let entry = entry.unwrap();
            println!(
                "{}: {} ({:?})",
                dir.display(),
                entry.path().display(),
                entry.file_type()
            );
        }

        for entry in std::fs::read_dir(dir).unwrap().flatten() {
            if let Ok(file_type) = entry.file_type() {
                if file_type.is_file() {
                    has_file = true;
                }
            }
        }

        assert_eq!(
            contains,
            has_file,
            "Directory {} does{} contain any files!",
            dir.display(),
            if has_file { "" } else { " not" }
        );
    }
}
