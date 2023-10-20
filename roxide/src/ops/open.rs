// editorconfig-checker-disable-file
// Reason: rustfmt formats closures with overly long parameter list
// not on the 4-character indentation boundary :(
use crate::db::{db::*, opt_txdb::*, txdb::*, ColumnFamilyHandle};
use crate::db_options::prelude::*;
use crate::error::{self, prelude::*};
use crate::ffi_try;
use crate::ffi_util::{path_to_cstring, path_to_string};
use crate::Cache;
use crate::{ffi, PessimisticTxOptions};
use std::collections::HashMap;
use std::ffi::CString;
use std::fs;
use std::path::Path;
use std::ptr;

pub trait DbOpen: Sized {
    /// Open a RocksDB database at a given path.  This might or might not exist, and might or might
    /// not be created if it doesn't exist, depending on the options selected.
    ///
    /// In any case if the path doesn't exist it's created.
    fn open<P, O>(path: P, db_options: O) -> Result<Self>
    where
        P: AsRef<Path>,
        O: Into<Option<DbOptions>>;

    /// Open a RocksDB database at a given path.  If the database doesn't exist at this path, the
    /// open will fail.
    fn open_existing<P, O>(path: P, db_options: O) -> Result<Self>
    where
        P: AsRef<Path>,
        O: Into<Option<DbOptions>>,
    {
        let mut options = db_options.into().unwrap_or_default();

        // Set the option to fail if the database is missing
        options.set_db_option_flag("create_if_missing", false);

        Self::open(&path, options)
    }

    /// Creates a new RocksDB database at the given path.  If a database already exists there, an
    /// error occurs.
    fn create_new<P, O>(path: P, db_options: O) -> Result<Self>
    where
        P: AsRef<Path>,
        O: Into<Option<DbOptions>>,
    {
        let mut options = db_options.into().unwrap_or_default();

        // Set the option to fail if the database exists
        options.set_db_option_flag("error_if_exists", true);

        Self::open(&path, options)
    }
}

pub trait DbOpenReadOnly: Sized {
    /// Open an existing RocksDB database read-only.  Any operations which cause the database to
    /// change, like `Put`, will fail.
    ///
    /// If a particular task does not require making changes to the database, opening read-only can
    /// improve performance as it disables the locking and synchronization which is normally in
    /// place to support concurrent reads and writes.
    fn open_readonly<P, O>(path: P, db_options: O, fail_if_log_file_exists: bool) -> Result<Self>
    where
        P: AsRef<Path>,
        O: Into<Option<DbOptions>>;
}

// Private helper that sets up the logging settings for a database if they aren't already
// configured
fn ensure_logging_configured(path: &Path, options: &mut DbOptions) {
    let (level, logger) = crate::logging::get_default_logger(path);

    options.set_default_logger(level, logger);
}

impl DbOpen for Db {
    /// Open a RocksDB database at a given path.  This might or might not exist, and might or might
    /// not be created if it doesn't exist, depending on the options selected.
    ///
    /// In any case if the path doesn't exist it's created.
    fn open<P, O>(path: P, db_options: O) -> Result<Self>
    where
        P: AsRef<Path>,
        O: Into<Option<DbOptions>>,
    {
        // Set up a default logger if there isn't already one

        // Call the RocksDB open API and get a database object
        let (db_ptr, cache, db_options, cfs) = ffi_open_helper(&path, db_options, Self::ffi_open)?;

        Ok(Db::new(path, DbHandle::new(db_ptr), cache, db_options, cfs))
    }
}

impl DbOpenReadOnly for Db {
    /// Open an existing RocksDB database read-only.  Any operations which cause the database to
    /// change, like `Put`, will fail.
    ///
    /// If a particular task does not require making changes to the database, opening read-only can
    /// improve performance as it disables the locking and synchronization which is normally in
    /// place to support concurrent reads and writes.
    fn open_readonly<P, O>(path: P, db_options: O, fail_if_log_file_exists: bool) -> Result<Self>
    where
        P: AsRef<Path>,
        O: Into<Option<DbOptions>>,
    {
        // Call the RocksDB open API and get a database object
        let (db_ptr, cache, db_options, cfs) = ffi_open_helper(
            &path,
            db_options,
            |options,
             path,
             num_column_families,
             column_family_names,
             column_family_options,
             column_family_handles| {
                unsafe {
                    ffi_try!(ffi::rocksdb_open_for_read_only_column_families(
                        options,
                        path,
                        num_column_families,
                        column_family_names,
                        column_family_options,
                        column_family_handles,
                        fail_if_log_file_exists as u8,
                    ))
                }
            },
        )?;

        Ok(Db::new(path, DbHandle::new(db_ptr), cache, db_options, cfs))
    }
}

cpp! {{
    #include "src/lib.h"
}}

impl DbOpen for TransactionDb {
    /// Opens a RocksDB database with transaction semantics
    ///
    /// TODO: Provide  Rust wrapper for the `TransactionDBOptions` type to control the transaction
    /// settings
    fn open<P, O>(path: P, db_options: O) -> Result<Self>
    where
        P: AsRef<Path>,
        O: Into<Option<DbOptions>>,
    {
        let db_options: DbOptions = db_options.into().unwrap_or_default();
        let transaction_options = db_options.tx_options();

        // Call the RocksDB open API and get a database object
        let (db_ptr, cache, db_options, cfs) = ffi_open_helper(
            &path,
            db_options,
            |options,
             path,
             num_column_families,
             column_family_names,
             column_family_options,
             column_family_handles| {
                unsafe {
                    let tx_options = ffi::rocksdb_transactiondb_options_create();

                    if let Some(PessimisticTxOptions {
                        default_lock_timeout_sec,
                        tx_lock_timeout_sec,
                    }) = transaction_options
                    {
                        // Default stripe count is 16.  A higher stripe count means less lock
                        // contention when the number of CPU cores running operations is
                        // substantially larger than 16.
                        // ffi::rocksdb_transactiondb_options_set_num_stripes(tx_options, 16);

                        ffi::rocksdb_transactiondb_options_set_default_lock_timeout(
                            tx_options,
                            (default_lock_timeout_sec * 1000).try_into().unwrap(),
                        );
                        ffi::rocksdb_transactiondb_options_set_transaction_lock_timeout(
                            tx_options,
                            (tx_lock_timeout_sec * 1000).try_into().unwrap(),
                        );
                    }

                    let result = ffi_try!(ffi::rocksdb_transactiondb_open_column_families(
                        options,
                        tx_options,
                        path,
                        num_column_families,
                        column_family_names,
                        column_family_options,
                        column_family_handles,
                    ));
                    ffi::rocksdb_transactiondb_options_destroy(tx_options);

                    result
                }
            },
        )?;

        Ok(TransactionDb::new(
            path,
            TransactionDbHandle::new(db_ptr),
            cache,
            db_options,
            cfs,
        ))
    }
}

impl DbOpen for OptimisticTransactionDb {
    fn open<P, O>(path: P, db_options: O) -> Result<Self>
    where
        P: AsRef<Path>,
        O: Into<Option<DbOptions>>,
    {
        // Call the RocksDB open API and get a database object
        let (db_ptr, cache, db_options, cfs) = ffi_open_helper(&path, db_options, Self::ffi_open)?;

        Ok(OptimisticTransactionDb::new(
            path,
            OptimisticTransactionDbHandle::new(db_ptr),
            cache,
            db_options,
            cfs,
        ))
    }
}

/// The different types of databases all have very similar `open` implementations, but different
/// enough that it requires some customization.  This helper implements the part that's the same
/// for all, which is mostly concerned with re-shaping the Rust types into C types, and then
/// converting the C output back to Rust.
#[allow(clippy::type_complexity)] // A 4-tuple is a complex type now?  These kids today...
pub(crate) fn ffi_open_helper<DB, P, O, F>(
    path: P,
    db_options: O,
    opener: F,
) -> Result<(
    ptr::NonNull<DB>,
    Cache,
    DbOptions,
    HashMap<String, ColumnFamilyHandle>,
)>
where
    P: AsRef<Path>,
    O: Into<Option<DbOptions>>,
    F: FnOnce(
        *const ffi::rocksdb_options_t,                 // database options
        *const libc::c_char,                           // database path
        libc::c_int,                                   // num_column_families
        *mut *const libc::c_char,                      // column_family_names
        *mut *const ffi::rocksdb_options_t,            // column_family_options
        *mut *mut ffi::rocksdb_column_family_handle_t, // column_family_handles
    ) -> Result<*mut DB>,
{
    let mut db_options = db_options.into().unwrap_or_default();

    // HACK: This is not at all elegant, but this function is the one place that all open
    // operations flow through where the path of the database is known, and the options struct can
    // be modified.  This is where we'll ensure that a logger is configured, the default one if the
    // caller hasn't overridden the logging impl.
    ensure_logging_configured(path.as_ref(), &mut db_options);

    // From the DBOptions make the RocksDB components
    let components = db_options.clone().into_components()?;

    fs::create_dir_all(&path).context(error::PathMkdirFailed {
        path: path.as_ref().to_owned(),
    })?;
    let path = path_to_string(path)?;
    let cpath = path_to_cstring(&path)?;

    // We need to store our CStrings in an intermediate vector
    // so that their pointers remain valid.
    let c_cf_names: Vec<CString> = components
        .column_families
        .iter()
        .map(|cf| CString::new(cf.name.as_bytes()).unwrap())
        .collect();

    let mut c_cf_name_ptrs: Vec<_> = c_cf_names.iter().map(|name| name.as_ptr()).collect();

    // These handles will be populated by DB.
    let mut cf_handle_ptrs: Vec<_> = vec![ptr::null_mut(); components.column_families.len()];

    let mut cfopt_ptrs: Vec<_> = components
        .column_families
        .iter()
        .map(|cf| cf.options.inner as *const _)
        .collect();

    let db = opener(
        components.options.inner,
        cpath.as_ptr(),
        components.column_families.len() as libc::c_int,
        c_cf_name_ptrs.as_mut_ptr(),
        cfopt_ptrs.as_mut_ptr(),
        cf_handle_ptrs.as_mut_ptr(),
    )?;

    let db = ptr::NonNull::new(db)
        .ok_or_else(|| Error::other_error(format!("failed to open database at '{}'", path)))?;

    // Convert the resulting handles into name/handle pairs, detecting errors along the way.
    // If Result::Err(...) in iterator is encountered, Result::Err(...) is returned immediately.
    let cf_map_result: Result<HashMap<_, _>> = cf_handle_ptrs
        .into_iter()
        .zip(components.column_families)
        .map(
            |(cf_handle, cf_descriptor)| match ptr::NonNull::new(cf_handle) {
                None => DatabaseError {
                    message: "Received null column family handle from DB.",
                }
                .fail(),
                Some(cf_handle) => Ok((cf_descriptor.name, ColumnFamilyHandle::new(cf_handle))),
            },
        )
        .collect();

    Ok((db, components.block_cache, db_options, cf_map_result?))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::TempDbPath;

    #[test]
    fn db_open() -> Result<()> {
        let path = TempDbPath::new();
        let mut options = crate::db_options::DbOptions::default();
        options.add_column_family("foo");

        let bto = options.get_default_cf_block_table_options();

        println!("BTO: {:?}", bto);
        println!("options: {:?}", options);
        let _db = Db::open(&path, options)?;

        Ok(())
    }

    #[test]
    fn txdb_open() -> Result<()> {
        let path = TempDbPath::new();
        let _db = TransactionDb::open(&path, None)?;

        Ok(())
    }

    #[test]
    fn opt_txdb_open() -> Result<()> {
        let path = TempDbPath::new();
        let _db = OptimisticTransactionDb::open(&path, None)?;

        Ok(())
    }
}
