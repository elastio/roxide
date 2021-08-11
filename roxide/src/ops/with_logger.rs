//! Reaches into the database or column family and looks into the C++ `Options` struct to get the
//! pointer to the C++ `Logger` class therein.  This is used in turn to access the Rust
//! `RocksDbLogger` impl and update its context so log events can more easily be identified by the
//! database and CF they pertain to.
use crate::logging::RocksDbLogger;
use crate::ops::GetDbPtr;

pub(crate) trait WithLogger {
    /// If the object is associated with a C++ RocksDB logger which wraps a Rust implementation in
    /// the form of `RocksDbLogger`, then calls a closure with a reference to that logger, or
    /// `None` if no such logger is configured.
    fn with_logger<R, F: FnOnce(Option<&(dyn RocksDbLogger + 'static)>) -> R>(&self, func: F) -> R
    where
        F: std::panic::UnwindSafe;
}

impl<DB: GetDbPtr> WithLogger for DB {
    fn with_logger<R, F: FnOnce(Option<&(dyn RocksDbLogger + 'static)>) -> R>(&self, func: F) -> R
    where
        F: std::panic::UnwindSafe,
    {
        let db_ptr = <Self as crate::ops::GetDbPtr>::get_db_ptr(self);

        unsafe { crate::logging::temp_logger_from_raw_db_ptr(db_ptr, func) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;
    use crate::db_options::DbOptions;
    use crate::logging::tests::TestLogger;
    use crate::ops::DbOpen;
    use crate::test::TempDbPath;
    use crate::Result;

    #[test]
    fn db_with_logger() -> Result<()> {
        let mut logger = TestLogger::new();
        logger.include_json = true;

        let mut options = DbOptions::default();
        options.set_logger(log::LevelFilter::Debug, logger);

        let path = TempDbPath::new();
        let db = Db::open(&path, options)?;

        // The same messages entry should be present now.
        #[allow(clippy::borrowed_box)]
        db.with_logger(|db_logger: Option<&(dyn RocksDbLogger + 'static)>| {
            let db_logger: &(dyn RocksDbLogger + 'static) =
                db_logger.expect("Expected a non-None logger");
            assert!(db_logger.include_json_events());
        });

        Ok(())
    }
}
