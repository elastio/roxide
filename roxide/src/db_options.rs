//! The `Options`/`BlockBasedOptions`/etc structs wrap the C bindings' opaque pointer types, and expose a
//! very verbose and tedious collection of option-specific setter functions.  These are never up to
//! date with the latest options available, and it's really inconvenient to load the options from a
//! config file or other dynamic source.
//!
//! So we take advantage of a feature in the RocksDB C++ API to set options arbitrarily from a set
//! of string name/value pairs.  Of course that means C++ STL, so the conversion is a bit hairy but
//! the increase in convenience is SO worth it.
use crate::error::prelude::*;
use crate::events;
use crate::ffi;
use crate::ffi_util::{self, DynTraitWrapper};
use crate::handle;
use crate::logging;
use crate::merge;
use crate::ops::StatsLevel;
use crate::Cache;
use cheburashka::logging::prelude::*;
use maplit::hashmap;
use num_traits::PrimInt;
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};
use std::hash::Hash;
use std::ptr;
use std::{
    borrow::Borrow,
    path::{Path, PathBuf},
};
use std::{collections::HashMap, ffi::CString};
use std::{ffi::c_void, sync::Arc};

// Re-export the RocksDB options types for convenience, so callers don't take any direct
// dependencies on the underlying `rocksdb` crate
pub use rocksdb::{
    self, BlockBasedIndexType, BlockBasedOptions, CompactOptions, DBCompactionStyle,
    DBCompressionType, DBPath, DBRecoveryMode, FlushOptions, MemtableFactory, Options,
    PlainTableFactoryOptions, ReadOptions, WriteOptions,
};

pub mod prelude {
    // Most code will want all of our extension traits on the RocksDB options types
    pub use super::{BlockBasedOptionsExt, ColumnFamilyDescriptor, DbOptions, OptionsExt};

    // And also needs the RocksDB option types themselves, which we re-export to make them look
    // like our own
    pub use super::{
        BlockBasedIndexType, BlockBasedOptions, CompactOptions, DBCompactionStyle,
        DBCompressionType, DBRecoveryMode, FlushOptions, MemtableFactory, Options,
        PlainTableFactoryOptions, ReadOptions, WriteOptions,
    };
}

const DEFAULT_BLOCK_CACHE: &str = "capacity=1G";

// In the C++ source file which the cpp macro will generate make sure the relevant includes are
// present
cpp! {{
    #include "src/db_options.h"
    #include "src/db_options.cpp"
}}

extern "C" {
    fn set_cf_path(options: *mut ffi::rocksdb_options_t, db_path: *mut ffi::rocksdb_dbpath_t);
}

// We use this `RocksObject` trait to plug in and type wrapping a RocksDB FFI handle type into the
// rest of our code.  All of our structs implement this themselves but the RocksDB options types
// that we re-use need to be wired in explicitly.

impl handle::RocksObject<ffi::rocksdb_writeoptions_t> for WriteOptions {
    fn rocks_ptr(&self) -> ptr::NonNull<ffi::rocksdb_writeoptions_t> {
        ptr::NonNull::new(self.inner).expect("write options")
    }
}

impl handle::RocksObject<ffi::rocksdb_readoptions_t> for ReadOptions {
    fn rocks_ptr(&self) -> ptr::NonNull<ffi::rocksdb_readoptions_t> {
        ptr::NonNull::new(self.inner).expect("read options")
    }
}

// Both ReadOptions and WriteOptions have default values that are global and static, for the very
// common case when no caller-defined options are provided
impl handle::RocksObjectDefault<ffi::rocksdb_writeoptions_t> for WriteOptions {
    fn default_object() -> &'static Self {
        static DEFAULT_WRITE_OPTIONS: OnceCell<WriteOptions> = OnceCell::new();
        DEFAULT_WRITE_OPTIONS.get_or_init(WriteOptions::default)
    }
}

impl handle::RocksObjectDefault<ffi::rocksdb_readoptions_t> for ReadOptions {
    fn default_object() -> &'static Self {
        static DEFAULT_READ_OPTIONS: OnceCell<ReadOptions> = OnceCell::new();
        DEFAULT_READ_OPTIONS.get_or_init(ReadOptions::default)
    }
}

impl handle::RocksObject<ffi::rocksdb_compactoptions_t> for CompactOptions {
    fn rocks_ptr(&self) -> ptr::NonNull<ffi::rocksdb_compactoptions_t> {
        ptr::NonNull::new(self.inner).expect("compaction options")
    }
}

impl handle::RocksObjectDefault<ffi::rocksdb_compactoptions_t> for CompactOptions {
    fn default_object() -> &'static Self {
        static DEFAULT_COMPACTION_OPTIONS: OnceCell<CompactOptions> = OnceCell::new();
        DEFAULT_COMPACTION_OPTIONS.get_or_init(CompactOptions::default)
    }
}

/// All databases always have a column family by this name, whether it's specified in the options or
/// not.
pub const DEFAULT_CF_NAME: &str = "default";

/// A descriptor for a RocksDB column family.
///
/// A description of the column family, containing the name and `Options`.
pub struct ColumnFamilyDescriptor {
    pub(crate) name: String,
    /// The column-family level RocksDB `Options` struct which overrides the database-level
    /// `Options`
    pub(crate) options: Options,

    /// The block cache used by this CF.
    ///
    /// This may be a unique cache specific to the CF, or it may be a clone of the DB-level cache
    /// in [`DbComponents::block_cache`], depending upon the [`DbOptions`] config which produces
    /// this descriptor.
    #[allow(dead_code)]
    // This is going to be used in the future to report per-CF block cache stats
    pub(crate) block_cache: Cache,
}

impl ColumnFamilyDescriptor {
    /// Create a new column family descriptor with the specified name and options.
    pub fn new<S>(name: S, options: Options, block_cache: Cache) -> Self
    where
        S: Into<String>,
    {
        ColumnFamilyDescriptor {
            name: name.into(),
            options,
            block_cache,
        }
    }

    /// The name of this column family.
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// The components necessary to create or open a RocksDB database
///
/// This is created from a [`DbOptions`] struct using [`DbOptions::into_components`], and contains
/// all of the lower-level structures and configuration needed to use `roxide-rocksdb` to open a
/// RocksDB database with configuration options as they were specified in the source `DbOptions`
/// instance
pub(crate) struct DbComponents {
    /// The database options struct (called `Options` because confusingly that's what the RocksDB
    /// C++ API uses)
    pub options: rocksdb::Options,

    /// The default block cache used by the entire database.
    pub block_cache: Cache,

    /// The column families in the DB, each with their own config
    pub column_families: Vec<ColumnFamilyDescriptor>,
}

/// Transaction options being applied to `TransactionDb`
#[derive(Clone, Serialize, Deserialize)]
pub struct PessimisticTxOptions {
    /// Lock timeout which is applied to standalone operations being executed without transaction.
    pub(crate) default_lock_timeout_sec: u64,

    /// Lock timeout for operations being executed in a transaction scope.
    pub(crate) tx_lock_timeout_sec: u64,
}

impl Default for PessimisticTxOptions {
    fn default() -> Self {
        Self {
            // RocksDB defaults (1 second for both parameters) are overridden in order to handle
            // concurrent operations updating thousands of keys with a common sub-set of keys among
            // the batches more efficiently avoiding unnecessary `LockTimeout`s with consequent retries.
            // These values may be revised in the future, but for now it looks like a reasonable default
            // which allows executing typical for ScaleZ batch update operations with conflicts
            // from about 100 threads in `Debug` build without lock timeouts.
            default_lock_timeout_sec: 60,
            tx_lock_timeout_sec: 60,
        }
    }
}

/// Struct that contains all of the information about a database other than it's path.  Not to be
/// confused with the `rocksdb::DBOptions` struct which is something entirely different and more
/// confusing
///
/// This struct is used to build `rocksdb::Options` (which wraps the C++ `Options` struct) and
/// `rocksdb::ColumnFamilyDescriptor` (which wraps the `Options` and name for each column family).
///
/// # Block Caches
///
/// There's some additional complexity related to configuring the block cache (or caches) on a
/// database.  This is necessary as part of #3026, to support the use of a single block cache
/// across multiple databases, and it's also what makes it possible to have separate block caches
/// for certain CFs while all the others use the default.
///
/// ## Database-level block cache
///
/// At the database level, setting a default cache with [`Self::set_default_block_cache`] will
/// override the cache specified in block cache options for the default column family, so that even
/// if `default_cf_options` contains a `block_based_table_factory` setting with a `block_cache`
/// value, that will be ignored and the [`crate::Cache`] instance passed to
/// [`Self::set_default_block_cache`] will be used as the block cache for the `default` CF and any
/// other CFs for which a separate block cache isn't provided.
///
/// If no default cache is provided with `set_default_block_cache`, then a 1GB block cache will be
/// created and used as the default for this database, and that cache will never be shared with any
/// other databases in the same process.
///
/// ## Column family-level block caches
///
/// The cache for a specific column family can be set with [`Self::set_column_family_block_cache`].
/// This makes it possible to share non-default caches between specific column families, possibly
/// even in different databases.  As with the default cache, setting a block cache with
/// `set_column_family_block_cache` will supercede the block cache specified in the
/// `block_based_table_factory` for that column family.  If no block cache was set with
/// `set_column_family_block_cache`, and a `block_cache` value is present in the column family's
/// `block_based_table_factory` value, then RocksDB will allocate a dedicated block cache for that
/// column family, which will be unique to that CF and cannot be shared with other databases in the
/// process.
#[derive(Clone, Serialize, Deserialize)]
pub struct DbOptions {
    /// Database-wide options
    db_options: HashMap<String, String>,

    /// Database-wide db_path, which will override the path specified in the call to `open*`
    /// functions if not set to `None`.
    ///
    /// If set this will also apply to all column families, unless a column family has its own path
    /// override in `column_family_paths`;
    db_path: Option<PathBuf>,

    /// Column family options which will apply to all column families
    default_cf_options: HashMap<String, String>,

    /// The block cache to use for all column families in this database, unless overridden for a
    /// specific CF.  If no cache is provided, the default is to allocate a single block cache for
    /// all CFs in the database, which is not shared with any other databases.
    default_block_cache: Option<Cache>,

    /// Transaction options applied to all CFs of a `TransactionDb`.
    /// This option is ignored for `OptimisticTransactionDb`.
    tx_options: Option<PessimisticTxOptions>,

    /// Additional (non-default) column families.  The key is the name of the CF, and the value is
    /// a `HashMap` of settings for that CF.  Each CF inherits settings from `default_cf_options`
    /// unless they are explicitly overridden.
    column_families: HashMap<String, HashMap<String, String>>,

    /// Custom database paths per column family.  The key is the same as `column_families`: the
    /// name of the CF.  If a given column family doesn't have a path in this field, then `db_path`
    /// is used; if `db_path` is `None`, then whatever path was passed to `open*` is used.
    column_family_paths: HashMap<String, PathBuf>,

    /// Custom block caches per column family.  The key is the same as `column_families`: the
    /// name of the CF.  If a given column family doesn't have a cache in this field, then
    /// `default_block_cache` is used; if `default_block_cache` is `None`, then a default block
    /// cache will be allocated automatically and used shared between the column families.
    column_family_block_caches: HashMap<String, Cache>,

    stats_level: StatsLevel,

    /// The merge operators, indexed by column family name
    ///
    /// This field cannot be serialized since `Fn` types are not generally speaking serializable.
    /// So callers must populate the merge operators explicitly even if the rest of the options are
    /// loaded from a config file
    #[serde(skip)]
    #[allow(clippy::type_complexity)] // The type is pretty straightforward as utilized
    merge_operators: HashMap<
        String,
        (
            String,
            Arc<dyn merge::MergeFn>,
            Option<Arc<dyn merge::MergeFn>>,
        ),
    >,

    /// An optional event listener which will receive various RocksDB notifications
    #[serde(skip)]
    event_listener: Option<Arc<dyn events::RocksDbEventListener>>,

    /// An optional logger which will be passed all RocksDB log messages at or above `log_level`
    #[serde(skip)]
    logger: Option<Arc<dyn logging::RocksDbLogger>>,

    /// The RocksDB internal log level, controlling what level of log messages get passed to
    /// `logger`.  If `logger` is `None`, this option has no effect
    #[serde(skip, default = "log::LevelFilter::max")]
    log_level: log::LevelFilter,
}

impl DbOptions {
    pub fn new<K, V, DBO, DCFO, CF>(
        db_options: DBO,
        default_cf_options: DCFO,
        column_families: CF,
    ) -> Self
    where
        K: ToString,
        V: ToString,
        DBO: Borrow<HashMap<K, V>>,
        DCFO: Borrow<HashMap<K, V>>,
        CF: Borrow<HashMap<K, HashMap<K, V>>>,
    {
        DbOptions {
            db_options: Self::make_owned_hash_map(db_options),
            db_path: None,
            default_cf_options: Self::make_owned_hash_map(default_cf_options),
            default_block_cache: None,
            tx_options: Some(PessimisticTxOptions::default()),

            column_families: column_families
                .borrow()
                .iter()
                .map(|(k, v)| (k.to_string(), Self::make_owned_hash_map(v)))
                .collect(),
            column_family_paths: HashMap::new(),
            column_family_block_caches: HashMap::new(),

            stats_level: StatsLevel::Disabled,
            merge_operators: HashMap::new(),
            event_listener: None,
            logger: None,
            log_level: log::LevelFilter::Error,
        }
    }

    /// Sets a single named database option to the specified value.  If the option is already set,
    /// the new value replaces the old value.
    pub fn set_db_option<K: ToString, V: ToString>(&mut self, option: K, value: V) {
        self.db_options
            .insert(option.to_string(), value.to_string());
    }

    /// Sets a flag value to `true` or `false`
    pub fn set_db_option_flag<K: ToString>(&mut self, option: K, value: bool) {
        self.set_db_option(option, if value { "true" } else { "false" });
    }

    /// Sets an integer value
    pub fn set_db_option_int<K: ToString, V: PrimInt + Display>(&mut self, option: K, value: V) {
        self.set_db_option(option, format!("{}", value));
    }

    /// Convenience helper to set the `create_if_missing` flag which is used often.  The advantage
    /// of using this method instead of setting the option explicitly is that you get compile-time
    /// verification that you remembered the option name correctly )
    pub fn create_if_missing(&mut self, value: bool) {
        self.set_db_option_flag("create_if_missing", value);
    }

    /// Convenience helper to set the `error_if_exists` flag which is used often.  The advantage
    /// of using this method instead of setting the option explicitly is that you get compile-time
    /// verification that you remembered the option name correctly )
    pub fn error_if_exists(&mut self, value: bool) {
        self.set_db_option_flag("error_if_exists", value);
    }

    /// Adds additional DB options, possibly overridding existing options if they have the same
    /// name
    pub fn add_db_options<K, V, O>(&mut self, options: O)
    where
        K: ToString,
        V: ToString,
        O: Borrow<HashMap<K, V>>,
    {
        let options = options.borrow();
        for (k, v) in options.iter() {
            self.db_options.insert(k.to_string(), v.to_string());
        }
    }

    /// Set a custom path for all DB files in all column families (unless a particular column
    /// family has a path override)
    pub fn set_db_path(&mut self, path: impl Into<PathBuf>) {
        self.db_path = Some(path.into());
    }

    /// Adds additional default CF options, possibly overridding existing options if they have the same
    /// name
    pub fn add_default_cf_options<K, V, O>(&mut self, options: O)
    where
        K: ToString,
        V: ToString,
        O: Borrow<HashMap<K, V>>,
    {
        let options = options.borrow();
        for (k, v) in options.iter() {
            self.default_cf_options.insert(k.to_string(), v.to_string());
        }
    }

    /// Gets the current default block-based table factory options
    pub fn get_default_cf_block_table_options(&self) -> HashMap<String, String> {
        if let Some(opts_str) = self.default_cf_options.get("block_based_table_factory") {
            Self::parse_options_string(opts_str.to_string())
        } else {
            // No such options
            HashMap::new()
        }
    }

    /// Sets the default CF block based table factory options.
    pub fn set_default_cf_block_table_options<K, V, O>(&mut self, options: O)
    where
        K: ToString,
        V: ToString,
        O: Borrow<HashMap<K, V>>,
    {
        self.default_cf_options.insert(
            "block_based_table_factory".to_string(),
            Self::build_options_string(options),
        );
    }

    /// Sets transaction options like lock timeouts for `TransactionDb` operating
    /// in pessimistic concurrency mode.
    pub fn set_pessimistic_tx_options(&mut self, options: PessimisticTxOptions) {
        self.tx_options = Some(options);
    }

    /// Sets the default number of bits per key to use for creation of bloom filters for all column
    /// families.
    ///
    /// Applies only if the default block-based table factory is used.
    pub fn set_default_cf_block_bloom_filter_bits(&mut self, bits_per_key: usize) -> Result<()> {
        let mut options = self.get_default_cf_block_table_options();
        options.insert(
            "filter_policy".to_string(),
            format!("bloomfilter:{}:false", bits_per_key),
        );

        self.set_default_cf_block_table_options(options);

        Ok(())
    }

    /// Set the [`crate::Cache`] instance that will be the default block cache for all CFs in this
    /// database which do not have a specific block cache of their own.
    ///
    /// If this is set, it overrides the default column family's `block_based_table_factory`
    /// parameter `block_cache`.
    ///
    /// Because `Cache` is very cheaply clonable, it's possible that this cache is also used as the
    /// block cache for other databases in the same process.  That's a supported use case, and all
    /// databases will contend for cache space as equals.
    pub fn set_default_block_cache(&mut self, cache: Cache) {
        self.default_block_cache = Some(cache);
    }

    /// Forcibly set `cache` as the block cache for this database, removing any column
    /// family-specific block cache instances or options.
    ///
    /// After this is called, it's guaranteed that all column families will use the specified cache
    /// and no other block cache will be in use.
    pub fn force_block_cache(&mut self, cache: Cache) {
        // First set this as the default
        self.set_default_block_cache(cache);

        // Now clear all CF-specific caches
        self.column_family_block_caches.clear();

        // And clear any `block_cache` options from the default and per-CF options maps
        let mut default_block_options = self.get_default_cf_block_table_options();
        default_block_options.remove("block_cache");
        self.set_default_cf_block_table_options(default_block_options);

        // Note the need to make owned copies of all of the CF names so that we can mutate `self`
        // inside the loop
        for name in self
            .column_families
            .keys()
            .map(|k| k.to_owned())
            .collect::<Vec<_>>()
        {
            let mut block_options = self
                .get_column_family_block_table_options(&name)
                .expect("BUG: This CF is known to be presenet");
            block_options.remove("block_cache");
            self.set_column_family_block_table_options(name, block_options)
                .expect("BUG: This CF is known to be presenet");
        }
    }

    pub fn set_stats_level(&mut self, level: StatsLevel) {
        self.stats_level = level;
    }

    /// Adds a non-default column family.  Unlike the other `add` methods, if `name` already exists
    /// as a column family, the options in `options` will completely replace all existing options
    /// associated with `name`.
    pub fn add_column_family<N>(&mut self, name: N)
    where
        N: ToString,
    {
        self.add_column_family_opts(name, HashMap::<String, String>::new());
    }

    /// Adds a non-default column family.  Unlike the other `add` methods, if `name` already exists
    /// as a column family, the options in `options` will completely replace all existing options
    /// associated with `name`.
    pub fn add_column_family_opts<N, K, V, O>(&mut self, name: N, options: impl Into<Option<O>>)
    where
        N: ToString,
        K: ToString,
        V: ToString,
        O: Borrow<HashMap<K, V>>,
    {
        let options = options
            .into()
            .map(|o| Self::make_owned_hash_map(o.borrow()))
            .unwrap_or_else(HashMap::new);
        self.column_families.insert(name.to_string(), options);
    }

    /// Override the path where data files for a given column family will be stored.
    pub fn set_column_family_path<N, P>(&mut self, name: N, path: P)
    where
        N: ToString,
        P: Into<PathBuf>,
    {
        self.column_family_paths
            .insert(name.to_string(), path.into());
    }

    /// Set the [`crate::Cache`] instance that will be the default block cache for a given column
    /// family.
    ///
    /// If this is set, it overrides the  column family's `block_based_table_factory` parameter
    /// `block_cache`.
    pub fn set_column_family_block_cache<N>(&mut self, name: N, cache: Cache)
    where
        N: ToString,
    {
        self.column_family_block_caches
            .insert(name.to_string(), cache);
    }

    /// Gets the current block-based table factory options for a previously-added column family.
    ///
    /// If there are no block-based table factory options for this CF, gets them from the default
    /// CF options.  If there are none there either, returns an empty map.
    pub fn get_column_family_block_table_options<N>(
        &self,
        cf_name: N,
    ) -> Result<HashMap<String, String>>
    where
        N: ToString,
    {
        let cf_name = cf_name.to_string();
        if let Some(cf_opts) = self.column_families.get(&cf_name) {
            if let Some(opts_str) = cf_opts.get("block_based_table_factory") {
                Ok(Self::parse_options_string(opts_str.to_string()))
            } else {
                // No such options
                Ok(self.get_default_cf_block_table_options())
            }
        } else {
            Err(Error::other_error(format!(
                "Column family '{}' not found",
                cf_name
            )))
        }
    }

    /// Sets the block based table factory options for a previously-added column family.  This
    /// completely replaces all block-based table factory options.  If that's not what you want,
    /// use `get_default_cf_block_table_options` to get the current options first and modify a copy
    /// of those options, or to set just one named option at a time use
    /// `set_default_cf_block_table_option`
    pub fn set_column_family_block_table_options<N, K, V, O>(
        &mut self,
        cf_name: N,
        options: O,
    ) -> Result<()>
    where
        N: ToString,
        K: ToString,
        V: ToString,
        O: Borrow<HashMap<K, V>>,
    {
        let cf_name = cf_name.to_string();
        if let Some(cf_opts) = self.column_families.get_mut(&cf_name) {
            if !options.borrow().is_empty() {
                cf_opts.insert(
                    "block_based_table_factory".to_string(),
                    Self::build_options_string(options),
                );
            } else {
                cf_opts.remove("block_based_table_factory");
            }

            Ok(())
        } else {
            Err(Error::other_error(format!(
                "Column family '{}' not found",
                cf_name
            )))
        }
    }

    pub fn set_column_family_block_table_option<N, K, V>(
        &mut self,
        cf_name: N,
        key: K,
        value: V,
    ) -> Result<()>
    where
        N: ToString,
        K: ToString,
        V: ToString,
    {
        let cf_name = cf_name.to_string();
        let mut block_opts = self.get_column_family_block_table_options(&cf_name)?;
        block_opts.insert(key.to_string(), value.to_string());
        self.set_column_family_block_table_options(&cf_name, block_opts)?;

        Ok(())
    }

    /// Sets the number of bits per key to use for creation of bloom filters.
    ///
    /// Applies only if the default block-based table factory is used.
    pub fn set_column_family_block_bloom_filter_bits(
        &mut self,
        cf_name: impl ToString,
        bits_per_key: usize,
    ) -> Result<()> {
        self.set_column_family_block_table_option(
            cf_name,
            "filter_policy",
            format!("bloomfilter:{}:false", bits_per_key),
        )
    }

    /// Sets the merge operator for a column family which should already have been added
    pub fn set_column_family_merge_operator(
        &mut self,
        cf_name: impl ToString,
        operator_name: impl ToString,
        full_merge_fn: Arc<dyn merge::MergeFn>,
        partial_merge_fn: Option<Arc<dyn merge::MergeFn>>,
    ) -> Result<()> {
        let cf_name = cf_name.to_string();
        if self.column_families.get(&cf_name).is_none() {
            Err(Error::other_error(format!(
                "Column family '{}' not found",
                cf_name
            )))
        } else {
            self.merge_operators.insert(
                cf_name,
                (operator_name.to_string(), full_merge_fn, partial_merge_fn),
            );

            Ok(())
        }
    }

    /// Sets an optional event listener which will receive events about RocksDB internal operations
    pub fn set_event_listener(&mut self, listener: impl events::RocksDbEventListener + 'static) {
        self.event_listener = Some(Arc::new(listener));
    }

    /// Sets the logger which will receive all of the RocksDB log messages corresponding to `level`
    pub fn set_logger(
        &mut self,
        level: log::LevelFilter,
        logger: impl logging::RocksDbLogger + 'static,
    ) {
        self.logger = Some(Arc::new(logger));
        self.log_level = level;
    }

    /// If there is no logger set on this options struct, sets the provided logger and level.  If
    /// logging is already configured on this options struct, does nothing.
    pub fn set_default_logger(
        &mut self,
        level: log::LevelFilter,
        logger: impl logging::RocksDbLogger + 'static,
    ) {
        if self.logger.is_none() {
            self.logger = Some(Arc::new(logger));
            self.log_level = level;
        }
    }

    pub(crate) fn tx_options(&self) -> Option<PessimisticTxOptions> {
        self.tx_options.clone()
    }

    /// Once the options are set correctly, this method is called to consume the struct and convert
    /// it into the raw components needed to create or open a RocksDB database with the underlying
    /// `roxide-rocksdb` crate.
    ///
    /// a `Options` struct with the database options, and a vector of
    /// `ColumnFamilyDescriptor` struct with the name and options of each column family.  This is
    /// the representation which RocksDB needs to operate on.
    #[allow(clippy::transmute_num_to_bytes)]
    pub(crate) fn into_components(mut self) -> Result<DbComponents> {
        // Immediately, make sure that there is a specific `Cache` instance for the DB and every
        // CF, removing any explicit `block_cache` options parameter in the process.
        //
        // After this, `self.default_block_cache` is guaranteed non-None, and there is guaranteed
        // to be a specific `Cache` in `self.column_family_block_caches` for every CF except for
        // default.
        self.instantiate_block_caches()?;

        let default_block_options = self.get_default_cf_block_table_options();
        let default_block_cache = self
            .default_block_cache
            .expect("BUG: instantiate_block_caches should have created a block cache already");

        let opts = Options::default();

        // Apply the DB and defualt CF options directly to this `Options` struct
        let mut opts = opts.set_db_options_from_map(&self.db_options)?;
        if let Some(db_path) = self.db_path.as_ref() {
            opts.set_db_path(db_path)?;
        }
        let mut opts =
            opts.set_cf_options_from_map(&self.default_cf_options, default_block_cache.clone())?;
        if let Some(logger) = self.logger {
            if let Some(level) = self.log_level.to_level() {
                opts.set_logger(level, logger);
            }
        }

        if let Some(event_listener) = self.event_listener {
            opts.set_event_listener(event_listener);
        }

        opts.set_default_checksum_gen_factory();

        // Always enable statistis collection.  This is not that expensive; it's the level of the
        // `Statistics` object that determines the overhead.  Initially it's `kDisable`
        unsafe {
            let opts_ptr = opts.inner;
            match self.stats_level {
                StatsLevel::Disabled => {
                    cpp!([opts_ptr as "rocksdb_options_t*"] {
                        cast_to_options(opts_ptr)->statistics = nullptr;
                    });
                }
                StatsLevel::Minimal | StatsLevel::Full => {
                    ffi::rocksdb_options_enable_statistics(opts_ptr);

                    let level = self.stats_level.as_rocks_level().unwrap();

                    cpp!([opts_ptr as "rocksdb_options_t*", level as "rocksdb::StatsLevel"] {
                        auto options_ptr = cast_to_options(opts_ptr);
                        assert(options_ptr->statistics);
                        options_ptr->statistics->set_stats_level(level);
                    });
                }
            }
        }

        // Callers should not generally create an explicit CF called `default`.  The `default` CF
        // always exists in all RocksDB databases, whether we add it or not.  However, for
        // consistency and to avoid special-casing code elsewhere related to this column group,
        // explicitly add it to the column families map, ensuring it is configured using the
        // default options
        if self.column_families.contains_key(DEFAULT_CF_NAME) {
            return Err(Error::other_error(format!(
                "The column family name `{}` is reserved and cannot be added explicitly",
                DEFAULT_CF_NAME
            )));
        }

        self.column_families
            .insert(DEFAULT_CF_NAME.to_string(), HashMap::new());
        self.column_family_block_caches
            .insert(DEFAULT_CF_NAME.to_string(), default_block_cache.clone());

        let mut merge_operators = self.merge_operators;
        let column_family_paths = self.column_family_paths;
        let mut column_family_block_caches = self.column_family_block_caches;

        // Create `ColumnFamilyDescriptor`s for every additional column family
        // If there were any errors, return the first one
        let cfs: Result<Vec<(String, Options)>, Error> =
            self.column_families
                .into_iter()
                .map(|(name, mut options)| {
                    // Take special care with `block_based_table_factory`, which unlike the other
                    // options we'll actually parse out of both the default and the CF-specific options
                    // hashes and use the combination of both.
                    let mut cf_block_options = default_block_options.clone();
                    if let Some(cf_options_string) = options.remove("block_based_table_factory") {
                        // There are some CF-specific block-based table factory settings, so
                        // overlay those on top of the default CF block based table options
                        for (key, value) in Self::parse_options_string(cf_options_string) {
                            cf_block_options.insert(key.to_string(), value.to_string());
                        }
                    };

                    // There is guaranteed to be a Cache object for every CF after
                    // `instantiate_block_caches` was called
                    let block_cache = column_family_block_caches
                        .get(&name)
                        .cloned()
                        .expect("BUG: instantiate_block_caches should have set this");

                    // Put back whatever options are left after removing `block_cache`
                    if !cf_block_options.is_empty() {
                        options.insert(
                            "block_based_table_factory".to_string(),
                            Self::build_options_string(&cf_block_options),
                        );
                    }

                    // Start from the default CF options, but layer on this family-specific options
                    let mut cf_opts = opts.set_cf_options_from_map(&options, block_cache)?;

                    // If there's a merge operator for this CF, apply that now
                    // Note we `remove` instead of `get` because the
                    // `set_merge_operator[_associative]` functions expect to be able to take
                    // ownership
                    if let Some((operator_name, full, partial)) = merge_operators.remove(&name) {
                        // Because MergeFn is a trait we store a boxed dyn trait, but
                        // `set_merge_operator` expects a specific `MergeFn` impl.
                        //
                        // If this is going to be passed to `set_merge_operator_associative` it
                        // also needs to be `Clone`, because whoever made that rust-rocksdb API
                        // change was a sadistic bastard.
                        let full = Arc::new(full);
                        let full =
                            move |key: &[u8],
                                  existing_value: Option<&[u8]>,
                                  operands: &crate::MergeOperands| {
                                full(key, existing_value, operands)
                            };
                        if let Some(partial) = partial {
                            // Thankfull the hack with Clone is only required for the `full` merge
                            // fn
                            let partial = move |key: &[u8],
                                                existing_value: Option<&[u8]>,
                                                operands: &crate::MergeOperands| {
                                partial(key, existing_value, operands)
                            };
                            cf_opts.set_merge_operator(&operator_name, full, partial);
                        } else {
                            // When partial merge function is `None` we assume that merge operator
                            // is associative.
                            // Which means:
                            //  - full value is undistinguishable from an operand,
                            //  - single provided merge function is going to be used as merge
                            // operator and as partial merge operator as well.
                            //
                            // Associative merge operator has a different setter. It, stupidly,
                            // requires that the MergeFn also be `Clone`
                            cf_opts.set_merge_operator_associative(&operator_name, full);
                        }
                    }

                    // If there's a custom path for this CF, apply that too
                    if let Some(cf_path) = column_family_paths.get(&name) {
                        debug!(cf_name = %name, cf_path = %cf_path.display(), "setting cf db path");
                        cf_opts.set_cf_path(cf_path)?;
                    }

                    Ok((name, cf_opts))
                })
                .collect();

        // Return the error if there is one otherwise unwrap
        let cfs = cfs?;

        let cf_descriptors: Vec<_> = cfs
            .into_iter()
            .map(|(name, options)| {
                let block_cache = column_family_block_caches
                    .remove(&name)
                    .expect("BUG: instantiate_block_caches should have set this");
                ColumnFamilyDescriptor::new(name, options, block_cache)
            })
            .collect();

        Ok(DbComponents {
            options: opts,
            block_cache: default_block_cache,
            column_families: cf_descriptors,
        })
    }

    /// Process the default CF settings and every per-CF setting, setting a specific `Cache`
    /// instance at the database level as well as every CF.
    ///
    /// After this completes, any `block_cache` option from the option maps will be removed, and
    /// there will be a concrete `Cache` instance set in `default_block_cache`, and each column
    /// family will also have a specific `Cache` instance assigned.  The default behavior if no
    /// cache settings were present in the options will be for all of those `Cache` instances to be
    /// clones of one single database-wide cache, however any existing `Cache` objects set at the
    /// DB or CF level, and any `block_cache` directives in the options, will be honored if
    /// present.
    fn instantiate_block_caches(&mut self) -> Result<()> {
        // Read the default CF block table options, removing `block_cache` if present, and then
        // re-write
        let mut default_block_options = self.get_default_cf_block_table_options();
        let block_cache_option = default_block_options.remove("block_cache");
        self.set_default_cf_block_table_options(default_block_options);

        // Create a block cache for the database, explicitly.
        //
        // Note that RocksDB will make one automatically, but if Rocks does that we won't be able
        // to get a hold of it, which means we can't ensure that other CFs re-use the same default
        // block cache.  By always creating it explicitly, we always have a `Cache` object we can
        // pass to the C++ BlockBasedOptions` for non-default CFs
        let default_block_cache = if let Some(cache) = self.default_block_cache.as_ref() {
            // A concrete `Cache` instance takes priority over a block_cache setting in the block
            // table options
            if let Some(overridden_cache) = block_cache_option {
                warn!(actual_default_block_cache = ?cache,
                    overridden_block_cache = %overridden_cache,
                    "The `block_cache` parameter in the default ColumnFamily options is being overriden by a `Cache` object passed at runtime");
            }

            cache.clone()
        } else {
            // Else caller didn't set a default block cache, so create a block cache object now
            let new_cache = if let Some(block_cache) = block_cache_option {
                // The default CF options have a `block_cache` value set, so use this to make the
                // default cache
                debug!(%block_cache, "Creating the default database block cache from a `block_cache` option");
                Cache::from_string(block_cache)?
            } else {
                // No block cache is set at all, so use the hard-coded default
                debug!(
                    block_cache = DEFAULT_BLOCK_CACHE,
                    "Creating the default database block cache using hard-coded default"
                );
                Cache::from_string(DEFAULT_BLOCK_CACHE)?
            };

            self.default_block_cache = Some(new_cache.clone());

            new_cache
        };

        // Now apply much the same logic but for every CF
        // Note the need to make owned copies of all of the CF names so that we can mutate `self`
        // inside the loop
        for name in self
            .column_families
            .keys()
            .map(|k| k.to_owned())
            .collect::<Vec<_>>()
        {
            let mut block_table_options = self.get_column_family_block_table_options(&name)?;
            let block_cache_option = block_table_options.remove("block_cache");
            self.set_column_family_block_table_options(&name, block_table_options)?;

            if let Some(block_cache) = self.column_family_block_caches.get(&name) {
                if let Some(overridden_cache) = block_cache_option {
                    // The block based table options included a `block_cache` option but
                    // that's being overridden by a specific block cache specified at
                    // runtime.  That's fine but make sure the user knows
                    warn!(actual_block_cache = ?block_cache,
                                overridden_block_cache = %overridden_cache,
                                column_family = %name,
                                "The column family `block_cache` parameter is being overriden by a `Cache` object passed at runtime");
                }
            } else {
                let cf_block_cache = if let Some(block_cache) = block_cache_option {
                    // No concrete `Cache` instance was provided for this CF, but it's
                    // `block_based_table_factory` options include an explicit `block_cache`
                    // directive so it should get its own block cache
                    debug!(%block_cache,
                            column_family = %name,
                            "Creating column family block cache based on `block_cache` parameter");
                    Cache::from_string(block_cache)?
                } else {
                    // No explicit Cache and no `block_cache` directive in the CF options, so
                    // just use the default cache
                    default_block_cache.clone()
                };
                self.column_family_block_caches.insert(name, cf_block_cache);
            };
        }

        Ok(())
    }

    fn char_at(s: &str, index: usize) -> char {
        s[index..index + 1].chars().next().unwrap()
    }

    /// Given some options in the RocksDB options string format, parses the options into a hash
    /// table.
    /// This function is the same as `StringToMap` from `options_helper.cc` in RocksDB, but written in Rust.
    /// Options example:
    ///   opts_str = "write_buffer_size=1024;max_write_buffer_number=2;"
    ///              "nested_opt={opt1=1;opt2=2};max_bytes_for_level_base=100"
    fn parse_options_string(opts: String) -> HashMap<String, String> {
        let mut opts = opts.trim();
        // If the input string starts and ends with "{...}", strip off the brackets
        while opts.len() > 2
            && Self::char_at(opts, 0) == '{'
            && Self::char_at(opts, opts.len() - 1) == '}'
        {
            opts = &opts[1..opts.len() - 1];
        }

        let mut opts_map = HashMap::new();
        let mut pos: usize = 0;
        while pos < opts.len() {
            let eq_pos = pos
                + opts[pos..]
                    .find('=')
                    .expect("Mismatched key value pair, '=' expected");
            let key = opts[pos..eq_pos].trim();
            if key.is_empty() {
                panic!("Empty key found");
            }

            let (end, value) = Self::next_token(opts, ';', eq_pos + 1);
            opts_map.insert(key.to_string(), value.to_string());
            match end {
                None => break,
                Some(end) => pos = end + 1,
            }
        }

        opts_map
    }

    /// This function is taken from `options_helper.cc` as well.
    /// Needed for `parse_options_string`.
    fn next_token(opts: &str, delimiter: char, mut pos: usize) -> (Option<usize>, &str) {
        while pos < opts.len() && Self::char_at(opts, pos).is_whitespace() {
            pos += 1;
        }

        // Empty value at the end
        if pos >= opts.len() {
            return (None, "");
        }

        if Self::char_at(opts, pos) == '{' {
            let mut count = 1;
            let mut brace_pos: usize = pos + 1;
            while brace_pos < opts.len() {
                if Self::char_at(opts, brace_pos) == '{' {
                    count += 1;
                } else if Self::char_at(opts, brace_pos) == '}' {
                    count -= 1;
                    if count == 0 {
                        break;
                    }
                }

                brace_pos += 1;
            }

            // found the matching closing brace
            if count == 0 {
                let token = opts[pos + 1..brace_pos].trim();
                // skip all whitespace and move to the next delimiter
                // brace_pos points to the next position after the matching '}'
                pos = brace_pos + 1;
                while pos < opts.len() && Self::char_at(opts, pos).is_whitespace() {
                    pos += 1;
                }
                if pos < opts.len() && Self::char_at(opts, pos) != delimiter {
                    panic!("Unexpected chars after nested options");
                }

                (Some(pos), token)
            } else {
                panic!("Mismatched curly braces for nested options");
            }
        } else {
            let end = opts[pos..].find(delimiter).map(|p| p + pos);
            let token = match end {
                None => opts[pos..].trim(),
                Some(end) => opts[pos..end].trim(),
            };

            (end, token)
        }
    }

    /// Constructs a RocksDB options string given a map of options
    fn build_options_string<K, V, O>(opts: O) -> String
    where
        K: ToString,
        V: ToString,
        O: Borrow<HashMap<K, V>>,
    {
        let opts = opts.borrow();
        let mut opts_str = String::new();

        for (key, value) in opts.iter() {
            // Inner option maps should be wrapped in braces.
            let mut value = value.to_string();
            if value.contains('=') {
                value = format!("{{{}}}", value);
            }
            opts_str.push_str(&format!("{}={};", key.to_string(), value));
        }

        opts_str
    }

    // Make a new struct with owned copies of all of the option strings
    fn make_owned_hash_map<K, V, O>(map: O) -> HashMap<String, String>
    where
        K: ToString,
        V: ToString,
        O: Borrow<HashMap<K, V>>,
    {
        map.borrow()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }
}

impl fmt::Debug for DbOptions {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.debug_struct("DbOptions")
            .field("db_options", &self.db_options)
            .field("db_path", &self.db_path)
            .field("default_cf_options", &self.default_cf_options)
            .field("default_block_cache", &self.default_block_cache)
            .field("column_families", &self.column_families)
            .field("column_family_paths", &self.column_family_paths)
            .field(
                "column_family_block_caches",
                &self.column_family_block_caches,
            )
            .field("stats_level", &self.stats_level)
            .field(
                "merge_operators",
                &self
                    .merge_operators
                    .iter()
                    .map(|(key, (operator_name, full_merge_fn, partial_merge_fn))| {
                        // render this name-value pair into something that can be formatted for debug output
                        (
                            key,
                            (
                                operator_name,
                                format!("{:p}", full_merge_fn),
                                format!("{:p}", partial_merge_fn),
                            ),
                        )
                    })
                    .collect::<Vec<_>>(),
            )
            .field("event_listener", &self.event_listener.is_some())
            .field("logger", &self.logger.is_some())
            .field("log_level", &self.log_level)
            .finish()
    }
}

impl PartialEq for DbOptions {
    fn eq(&self, other: &Self) -> bool {
        self.db_options == other.db_options
            && self.default_cf_options == other.default_cf_options
            && self.default_block_cache == other.default_block_cache
            && self.column_families == other.column_families
            && self.column_family_block_caches == other.column_family_block_caches
            && self.stats_level == other.stats_level
            && self.merge_operators.keys().collect::<Vec<_>>()
                == other.merge_operators.keys().collect::<Vec<_>>()
    }
}

impl Default for DbOptions {
    fn default() -> Self {
        /***************************************
         * IMPORTANT REMINDER!!!!!
         *
         * These default options are used in `roxide`, but they are overridden in `scalez-kv` in
         * the `default.toml` config file.
         *
         * If changing these defaults, think if this should also be the default for scalez.  If so
         * do not fail to make the change in `default.toml` in the `scalez-kv` project as well.
         * */
        let db_opts = hashmap! {
            "max_background_compactions" => "4",
            "max_background_flushes" => "4",
            "max_subcompactions" => "4",
            "bytes_per_sync" => "8M",
            "create_if_missing" => "true",
            "create_missing_column_families" => "true",
            "write_dbid_to_manifest" => "true",

            // https://github.com/facebook/rocksdb/wiki/Atomic-flush
            //
            // By default all CFs should be flushed together.  Otherwise writes to one CF might be
            // persisted to disk and writes to another CF might be lost, in the event of a crash of
            // the process
            "atomic_flush" => "true"
        };
        let default_cf_opts = hashmap! {
            // Don't adhere to a strict hierarchy of L0->L1->L2..., optionally skip levels if
            // there's not enough data to justify them all.
            //
            // https://github.com/facebook/rocksdb/wiki/Leveled-Compaction
            "level_compaction_dynamic_level_bytes" => "true",

            // Because we use `level_compaction_dynamic_level_bytes`, this is the target size for
            // the highest level.  The size of other levels will be computed as a function of this
            // one.  See the article above about Leveled Compaction for details
            "max_bytes_for_level_base" => "512M", // write_buffer_size*min_write_buffer_number_to_merge*level0_file_num_compaction_trigger

            // https://rocksdb.org/blog/2016/01/29/compaction_pri.html
            //
            // kMinOverlappingRatio is a new algorithm (now the default but here for explicitness)
            // which minimizes write amplification.  It pairs well with
            // `level_compaction_dynamic_level_bytes`
            "compaction_pri" => "kMinOverlappingRatio",

            // Determines the size of the memtables and the L0 files which they are flushed to
            "write_buffer_size" => "128M",

            // The target file size for L1, which is the first level in which each SST file is
            // non-overlapping.  By keeping this equal to `write_buffer_size` we maintain a
            // roughly one-to-one mapping of L0 and L1 file counts (although of course the L0 and
            // L1 files will be totally different because L0 is not sorted and thus all L0 files
            // must be scanned for every key during reads).
            "target_file_size_base" => "128M", // should be the same as write_buffer_size

            // How many `write_buffer_size` memtables to keep active in memory at once.  Setting
            // this to at least 2 ensures when a memtable fills up and needs to be flushed to disk,
            // there's another memtable available to receive writes.  Once this number of memtables
            // are full writes will stall until a flush completes.  It might be necessary to
            // increase this further if the write rate exceeds the speed at which we can flush to
            // disk.
            "max_write_buffer_number" => "2",

            // The minimum number of full memtables before writing.  If this number is more than 1,
            // the memtables will be merged before flushing.  This would be an optimization in
            // cases when the same key is being written multiple times with different values, but
            // it comes at the price of incurring that merge overhead and also requires `Get`
            // operations to traverse all memtables.  This is almost never worth it particularly in
            // our kinds of use cases
            "min_write_buffer_number_to_merge" => "1",

            // The number of L0 SST files which will trigger compaction into L1.  Since L0 files
            // are just memtables flushed to disk, they almost certainly overlap eachother so they
            // have to be compacted as a group.  Higher levels can be compacted in parallel one
            // file at a time.
            //
            // https://github.com/facebook/rocksdb/wiki/Leveled-Compaction
            "level0_file_num_compaction_trigger" => "4",

            // Compress all data blocks with LZ4, except the bottommost levl which will be
            // compressed with zstd.  zstd is slower but more efficient, so we don't want to pay
            // that price until data is old and stable enough to make it to the bottom level.  Once
            // there it can't be moved any further.
            "compression" => "kLZ4Compression",
            "bottommost_compression" => "kZSTD",

            // There are a lot of high-impact tunables here.  There's no one place in the docs to
            // tell you how to set what.  These settings are a result of reading a lot of wiki
            // pages, and experimentation.
            "block_based_table_factory" => concat!(
                // This is the size of a data block.  Index/filter blocks are constructed
                // differently.  Data are read from the database in units of this size.
                "block_size=32K;",

                // The in-memory cache of (uncompressed) blocks.  Of course the OS will also
                // maintain its own page cache, but that will hold the compressed on-disk version
                // of the blocks.
                //
                // Obviously, depending on the available memory, this should be adjusted upwards as
                // much as possible for any read-intensive workloads
                "block_cache=1G;",

                // use the latest table format
                // https://rocksdb.org/blog/2019/03/08/format-version-4.html
                //
                // This was `format_version=4` but this post makes a passing reference to the fact
                // that version 5, introduced in 6.6, improves the Bloom filter impl:
                // https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter
                //
                // See also https://github.com/facebook/rocksdb/wiki/Rocksdb-BlockBasedTable-Format
                "format_version=5;",

                // Use two-level partitioned indexes https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters
                // This generates a small index-of-indexes to determine which index partition
                // applies to a given range of keys.  That means two index lookups instead of one,
                // but it also means there's a smaller top-level index that we can always pin into
                // memory.
                "index_type=kTwoLevelIndexSearch;",

                // Enable partioning of filters within an SST (goes with two level index search)
                // https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters
                "partition_filters=true;",

                // Block size for index partitions; this seems like a reasonable default
                // https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters
                "metadata_block_size=4096;",

                // Store the index and bloom filter blocks in the cache
                "cache_index_and_filter_blocks=true;",

                // When deciding what blocks to evict from the cache, prioritize keeping index and
                // filter blocks over data blocks, on the theory that they will be read often
                // anyway and have a disproportionately high impact on read performance when
                // they're already in the cache
                "cache_index_and_filter_blocks_with_high_priority=true;",

                // DEPRECATED: This option will be removed in a future version. For now, this
                // option still takes effect by updating
                // `MetadataCacheOptions::top_level_index_pinning` when it has the
                // default value, `PinningTier::kFallback`.
                //
                // The updated value is chosen as follows:
                //
                // - `pin_top_level_index_and_filter == false` ->
                //   `PinningTier::kNone`
                // - `pin_top_level_index_and_filter == true` ->
                //   `PinningTier::kAll`
                //
                // To migrate away from this flag, explicitly configure
                // `MetadataCacheOptions` as described above.
                // Similar to `pin_l0...` except `pin_l0..` pins the entire filter/index
                // blocks for L0 data files.  This pins the top-level (of the two levels
                // enabled by `index_type`) index and filter _OF EACH SST_, on the theory
                // that the top level indexes are very small, and having them always in
                // cache lets us very quickly identify which partition(s) in a given SST
                // may contain the data we need.  The second level index/filter blocks are also
                // cached per `cache_index_and_filter_blocks`, but those blocks may or may not be
                // pushed out of the cache depending on cache pressure.
                // "pin_top_level_index_and_filter=true;",

                // DEPRECATED: This option will be removed in a future version. For now, this
                // option still takes effect by updating each of the following variables that
                // has the default value, `PinningTier::kFallback`:
                //
                // - `MetadataCacheOptions::partition_pinning`
                // - `MetadataCacheOptions::unpartitioned_pinning`
                //
                // The updated value is chosen as follows:
                //
                // - `pin_l0_filter_and_index_blocks_in_cache == false` ->
                //   `PinningTier::kNone`
                // - `pin_l0_filter_and_index_blocks_in_cache == true` ->
                //   `PinningTier::kFlushedAndSimilar`
                //
                // To migrate away from this flag, explicitly configure
                // `MetadataCacheOptions` as described above.
                // Pin the L0 filter and index blocks in the cache so they are never pushed out by
                // data blocks
                // "pin_l0_filter_and_index_blocks_in_cache=true;",

                // New `metadata_cache_options` configuration option, introduced in RocksDb 6.15.0:
                // https://github.com/facebook/rocksdb/blob/master/HISTORY.md#6150-2020-11-13
                // See comments to `pin_l0_filter_and_index_blocks_in_cache` and
                // `pin_top_level_index_and_filter` above for details.
                "metadata_cache_options={",
                    "top_level_index_pinning=kAll;",
                    "partition_pinning=kFlushedAndSimilar;",
                    "unpartitioned_pinning=kFlushedAndSimilar;",
                "};",

                // Use 15 bits for the bloom filter.  The `false` falue corresponds to the second
                // parameter of the `NewBloomFilterPolicy` method, which is called
                // `use_block_based_builder`.  `use_block_based_builder = false` tells RocksDB to
                // use the new format in which there is one bloom filter per SST file.
                //
                // The default was 10, but improvements in Bloom filter performance in
                // `format_version=5` mean that by increasing to 15 bits, the false positive rate
                // is now a little more than 1 in 1000 queries; at 10 bits it was 1 in 100.  That's
                // a big performance win for 5 more bits per key of overhead.
                //
                // You can read more about this at
                // https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter
                //
                // TODO: 6.15 adds Ribbon filters (activated by replacing `bloomfilter` with
                // `ribbonfilter`).  According to the docs this adds a bit of CPU overhead and
                // reduces memory usage 30% for the same false positive rate as a bloom.  Need to
                // experiment with this and see if it makes sense for our use case.
                "filter_policy=bloomfilter:15:false;",

                // https://rocksdb.org/blog/2018/08/23/data-block-hash-index.html
                "data_block_index_type=kDataBlockBinaryAndHash;",

                // See the article above for details
                // The lower this ratio the higher the space overhead but the less likely point
                // lookups are to fall back to a binary search
                // According to FB testing, a ratio of 1 imposes overhead of 1 byte per key, and
                // results in a lookup falling back to a binary search 52% of the time.  A lower
                // ratio will impose a higher per-key overhead, and a lower probability of falling
                // back to a binary search.
                "data_block_hash_table_util_ratio=0.5;"
            ),

        };

        DbOptions::new(db_opts, default_cf_opts, HashMap::new())
    }
}

/// Generalized helper for creating some arbitrary `Options`-like struct initialized with values
/// from a map of name/value pairs.
unsafe fn set_options_from_map<
    OptionT: Default,
    K: AsRef<str> + Into<Vec<u8>>,
    V: AsRef<str> + Into<Vec<u8>>,
    F: FnOnce(&OptionT, &OptionT, *const c_void) -> *mut ::libc::c_char,
>(
    options: &OptionT,
    map: &HashMap<K, V>,
    setter_func: F,
) -> Result<OptionT> {
    let new_options = OptionT::default();

    let map_ptr = ffi_util::hashmap_to_stl_unordered_map(map);

    let err = setter_func(options, &new_options, map_ptr);

    ffi_util::free_unordered_map(map_ptr);

    ffi_util::make_result(new_options, err)
}

pub trait BlockBasedOptionsExt {
    /// Populates a `BlockBasedOptions` structure with options obtained from a `HashMap`.
    ///
    /// This has the advantage of always accepting the latest options, no need to wait for them to
    /// be added to the RocksDB C bindings or to the `rust-rocksdb` wrapper.
    ///
    /// # Example
    ///
    /// ```
    /// use std::collections::HashMap;
    /// use roxide::{BlockBasedOptions, BlockBasedOptionsExt};
    ///
    /// let mut options = HashMap::new();
    /// options.insert("filter_policy", "bloomfilter:4:true");
    /// options.insert("cache_index_and_filter_blocks", "true");
    /// let _opt = BlockBasedOptions::default().set_options_from_map(&options).unwrap();
    /// ```
    fn set_options_from_map<K: AsRef<str> + Into<Vec<u8>>, V: AsRef<str> + Into<Vec<u8>>>(
        &self,
        map: &HashMap<K, V>,
    ) -> Result<BlockBasedOptions>;
}

impl BlockBasedOptionsExt for BlockBasedOptions {
    fn set_options_from_map<K: AsRef<str> + Into<Vec<u8>>, V: AsRef<str> + Into<Vec<u8>>>(
        &self,
        map: &HashMap<K, V>,
    ) -> Result<BlockBasedOptions> {
        unsafe {
            set_options_from_map::<BlockBasedOptions, _, _, _>(
                self,
                map,
                |options, new_options, map_ptr| {
                    let options_ptr = options.inner;
                    let new_options_ptr = new_options.inner;

                    cpp!([options_ptr as "const rocksdb::BlockBasedTableOptions*", new_options_ptr as "rocksdb::BlockBasedTableOptions*", map_ptr as "const OptionsMap*"] -> *mut ::libc::c_char as "const char*" {
                        // TODO: the addition of a ConfigOptions argument to this function was
                        // added when we upgraded to Rocks 8.0 (though it might have landed in
                        // Rocks before that).  Previous that argument wasn't passed.  THe way the
                        // surrounding Rust code is written, there is no ConfigOptions instance
                        // available.  So we use the default value here.  Hopefully that's correct
                        auto config_options = rocksdb::ConfigOptions();

                        auto status = rocksdb::GetBlockBasedTableOptionsFromMap(
                            config_options,
                            *options_ptr,
                            *map_ptr,
                            new_options_ptr
                        );

                        if (status.ok()) {
                            return nullptr;
                        } else {
                            return strdup(status.getState());
                        }
                        return nullptr;
                    })
                },
            )
        }
    }
}

pub trait OptionsExt {
    /// Populates a `Options` structure with database options obtained from a `HashMap`.
    ///
    /// This has the advantage of always accepting the latest options, no need to wait for them to
    /// be added to the RocksDB C bindings or to the `rust-rocksdb` wrapper.
    ///
    /// # Example
    ///
    /// ```
    /// use std::collections::HashMap;
    /// use roxide::{Options, OptionsExt};
    ///
    /// let mut options = HashMap::new();
    /// options.insert("allow_mmap_reads", "true");
    /// options.insert("create_if_missing", "true");
    /// let opt = Options::default();
    /// let _opt = opt.set_db_options_from_map(&options).unwrap();
    /// ```
    fn set_db_options_from_map<K: AsRef<str> + Into<Vec<u8>>, V: AsRef<str> + Into<Vec<u8>>>(
        &self,
        map: &HashMap<K, V>,
    ) -> Result<Options>;

    /// Populates a `Options` structure with column family options obtained from a `HashMap`.
    ///
    /// This has the advantage of always accepting the latest options, no need to wait for them to
    /// be added to the RocksDB C bindings or to the `rust-rocksdb` wrapper.
    ///
    /// This function does not expect to see `block_cache` in `block_based_table_factory` options,
    /// because the caller should have already stripped that option out (if present) and replaced
    /// it with a concrete `Cache` instance in the `block_cache` argument.
    ///
    /// This doesn't support any CF types other than those created with `BlockBasedTableFactory`
    /// because of this requirement.
    ///
    /// # Example
    ///
    /// ```
    /// use std::collections::HashMap;
    /// use roxide::{Cache, Options, OptionsExt};
    ///
    /// let cache = Cache::with_capacity(1_000_000).unwrap();
    /// let mut options = HashMap::new();
    /// options.insert("report_bg_io_stats", "true");
    /// options.insert("max_compaction_bytes", "1M");
    /// let opt = Options::default();
    /// let _opt = opt.set_cf_options_from_map(&options, cache).unwrap();
    /// ```
    fn set_cf_options_from_map<
        K: AsRef<str> + Into<Vec<u8>> + Eq + Hash,
        V: AsRef<str> + Into<Vec<u8>>,
    >(
        &self,
        map: &HashMap<K, V>,
        block_cache: Cache,
    ) -> Result<Options>;

    /// Set the `db_paths` to a single path `path` without any size limit.
    ///
    /// NB: If this `Options` struct is being used to set column family options, then this DB path
    /// is ignored by RocksDB.  Instead you must used `set_cf_path`
    fn set_db_path<P: AsRef<Path>>(&mut self, path: P) -> Result<()>;

    /// Set the `cf_paths` to a single path `path` without any size limit.
    ///
    /// NB: If this `Options` struct is being used to set database options, then this CF path
    /// is ignored by RocksDB.  Instead you must used `set_db_path`
    fn set_cf_path<P: AsRef<Path>>(&mut self, path: P) -> Result<()>;

    /// Sets a log level and logger implementation using a Rust trait.
    ///
    /// The specified `level` is the minimum level that will be logged.  Log events below this
    /// level will be skipped at the source level and thus incur almost no overhead.  Log events at
    /// this level or above will be passed to the logging trait.
    fn set_logger(&mut self, min_level: log::Level, logger: Arc<dyn logging::RocksDbLogger>);

    fn inner_logger(&self) -> *mut ffi::rocksdb_options_t;

    /// If there is a C++ logger configured, temporarily converts it back to a Rust impl and passes
    /// that impl to a closure for additional customization.
    ///
    /// If there is no logger configured, `None` is passed to the closure.
    fn with_logger<R, F: FnOnce(Option<&dyn logging::RocksDbLogger>) -> R>(&mut self, func: F) -> R
    where
        F: std::panic::UnwindSafe,
    {
        let options_ptr = self.inner_logger();
        let raw_logger_ptr = unsafe {
            cpp!([options_ptr as "rocksdb_options_t*"] -> *mut std::ffi::c_void as "void*" {
                void* rust_boxed_logger = NULL;

                auto logger_ptr = cast_to_options(options_ptr)->info_log;
                if (logger_ptr) {
                    auto logger = static_cast<RustLogger*>(logger_ptr.get());

                    rust_boxed_logger = logger->get_rust_boxed_logger();
                }

                return rust_boxed_logger;
            })
        };

        if raw_logger_ptr.is_null() {
            func(None)
        } else {
            unsafe {
                logging::CppLoggerWrapper::with_raw_boxed_logger(raw_logger_ptr, |logger| {
                    func(Some(logger))
                })
            }
        }
    }

    /// Sets an event listener to be notified of RocksDB internal events
    fn set_event_listener(&mut self, listener: Arc<dyn events::RocksDbEventListener>);

    /// Sets default crc32 checksum generator factory
    fn set_default_checksum_gen_factory(&mut self);
}

impl OptionsExt for Options {
    fn set_db_options_from_map<K: AsRef<str> + Into<Vec<u8>>, V: AsRef<str> + Into<Vec<u8>>>(
        &self,
        map: &HashMap<K, V>,
    ) -> Result<Options> {
        unsafe {
            set_options_from_map::<Options, _, _, _>(self, map, |options, new_options, map_ptr| {
                let options_ptr = options.inner;
                let new_options_ptr = new_options.inner;

                cpp!([options_ptr as "rocksdb_options_t*", new_options_ptr as "rocksdb_options_t*", map_ptr as "const OptionsMap*"] -> *mut ::libc::c_char as "const char*" {
                    // Confusingly, the `Options` class inherits from both `DBOptions` and
                    // `ColumnFamilyOptions`, which is why this seemingly-wrong code compiles
                    auto options_class_ptr = cast_to_options(options_ptr);
                    const rocksdb::ColumnFamilyOptions& existing_cf_options = *static_cast<const rocksdb::ColumnFamilyOptions*>(options_class_ptr);
                    auto new_db_options = rocksdb::DBOptions();

                    // TODO: the addition of a ConfigOptions argument to this function was
                    // added when we upgraded to Rocks 8.0 (though it might have landed in
                    // Rocks before that).  Previous that argument wasn't passed.  THe way the
                    // surrounding Rust code is written, there is no ConfigOptions instance
                    // available.  So we use the default value here.  Hopefully that's correct
                    auto config_options = rocksdb::ConfigOptions();
                    auto status = rocksdb::GetDBOptionsFromMap(
                        config_options,
                        *options_class_ptr,
                        *map_ptr,
                        &new_db_options
                    );

                    if (status.ok()) {
                        // Create a new Options struct which is a combionation of the newly
                        // populated DBOptions and the ColumnFamilyOptions that were already in the
                        // old Options struct
                        *cast_to_options(new_options_ptr) = rocksdb::Options(new_db_options, existing_cf_options);
                        return nullptr;
                    } else {
                        return strdup(status.getState());
                    }
                    return nullptr;
                })
            })
        }
    }

    fn set_cf_options_from_map<
        K: AsRef<str> + Into<Vec<u8>> + Eq + Hash,
        V: AsRef<str> + Into<Vec<u8>>,
    >(
        &self,
        map: &HashMap<K, V>,
        block_cache: Cache,
    ) -> Result<Options> {
        let mut map = map
            .iter()
            .map(|(k, v)| (k.as_ref().to_owned(), v.as_ref().to_owned()))
            .collect::<HashMap<_, _>>();

        // Remove the `block_based_table_factory` option, if present, because we'll handle that
        // separately
        let block_based_options_map = map
            .remove("block_based_table_factory")
            .map(|value| {
                // Parse this single string into a hashmap of options being passed to the block
                // based table factory
                DbOptions::parse_options_string(value)
            })
            .unwrap_or_default();

        // Code in `into_components` should have stripped `block_cache` out before it gets this
        // far, so just make sure
        debug_assert!(!block_based_options_map.contains_key("block_cache"));

        // Set the block_based_table_factory options (other than `block_cache`) using a map of
        // strings, and the block cache set explicitly with a pointer to the cache
        let block_based_options =
            BlockBasedOptions::default().set_options_from_map(&block_based_options_map)?;

        unsafe {
            ffi::rocksdb_block_based_options_set_block_cache(
                block_based_options.inner,
                block_cache.rocksdb_cache_t(),
            );
        }

        // Set all of these block-based options on the options struct
        unsafe {
            ffi::rocksdb_options_set_block_based_table_factory(
                self.inner,
                block_based_options.inner,
            );
        }

        // Now all other CF options, other than the block-based ones, can be applied
        unsafe {
            set_options_from_map::<Options, _, _, _>(self, &map, |options, new_options, map_ptr| {
                let options_ptr = options.inner;
                let new_options_ptr = new_options.inner;

                cpp!([options_ptr as "rocksdb_options_t*", new_options_ptr as "rocksdb_options_t*", map_ptr as "const OptionsMap*"] -> *mut ::libc::c_char as "const char*" {
                    // Confusingly, the `Options` class inherits from both `DBOptions` and
                    // `ColumnFamilyOptions`, which is why this seemingly-wrong code compiles
                    auto options_class_ptr = cast_to_options(options_ptr);
                    const rocksdb::DBOptions& existing_db_options = *static_cast<const rocksdb::DBOptions*>(options_class_ptr);
                    auto new_cf_options = rocksdb::ColumnFamilyOptions();

                    // TODO: the addition of a ConfigOptions argument to this function was
                    // added when we upgraded to Rocks 8.0 (though it might have landed in
                    // Rocks before that).  Previous that argument wasn't passed.  THe way the
                    // surrounding Rust code is written, there is no ConfigOptions instance
                    // available.  So we use the default value here.  Hopefully that's correct
                    auto config_options = rocksdb::ConfigOptions();
                    auto status = rocksdb::GetColumnFamilyOptionsFromMap(
                        config_options,
                        *options_class_ptr,
                        *map_ptr,
                        &new_cf_options
                    );

                    if (status.ok()) {
                        // Create a new Options struct which is a combionation of the newly
                        // populated ColumnFamilyOptions and the DBOptions that were already in the
                        // old Options struct
                        *cast_to_options(new_options_ptr) = rocksdb::Options(existing_db_options, new_cf_options);
                    } else {
                        return strdup(status.getState());
                    }
                    return nullptr;
                })
            })
        }
    }

    fn set_db_path<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        let rocks_path = DBPath::new(path, 0).with_context(|| crate::error::RustRocksDbError {})?;
        self.set_db_paths(&[rocks_path]);

        Ok(())
    }

    fn set_cf_path<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        let path = CString::new(path.as_ref().to_string_lossy().as_bytes()).unwrap();
        let dbpath = unsafe { ffi::rocksdb_dbpath_create(path.as_ptr(), 0) };
        assert!(
            !dbpath.is_null(),
            "Memory allocation failure creating DB path for '{}'",
            path.to_string_lossy()
        );

        // Reach into this options struct to set cf_paths.  That's not exposed via the C bindings
        unsafe {
            set_cf_path(self.inner, dbpath);
            ffi::rocksdb_dbpath_destroy(dbpath);
        }

        Ok(())
    }

    #[allow(clippy::transmute_num_to_bytes)]
    fn set_logger(&mut self, min_level: log::Level, logger: Arc<dyn logging::RocksDbLogger>) {
        let logger = logging::CppLoggerWrapper::wrap(logger);

        let options_ptr = self.inner;
        let boxed_logger = logger.into_raw_void();
        let rocksdb_level: u8 = match min_level {
            log::Level::Debug | log::Level::Trace => 0,
            log::Level::Info => 1,
            log::Level::Warn => 2,
            log::Level::Error => 3,
        };

        unsafe {
            cpp!([options_ptr as "rocksdb_options_t*", boxed_logger as "void*", rocksdb_level as "rocksdb::InfoLogLevel"] {
                cast_to_options(options_ptr)->info_log = std::shared_ptr<RustLogger>(new RustLogger(rocksdb_level, boxed_logger));
            })
        }
    }

    fn inner_logger(&self) -> *mut ffi::rocksdb_options_t {
        self.inner
    }

    /// Sets an event listener to be notified of RocksDB internal events
    fn set_event_listener(&mut self, listener: Arc<dyn events::RocksDbEventListener>) {
        let listener = events::CppListenerWrapper::wrap(listener);

        let options_ptr = self.inner;
        let boxed_listener = listener.into_raw_void();

        unsafe {
            cpp!([options_ptr as "rocksdb_options_t*", boxed_listener as "void*"] {
                cast_to_options(options_ptr)->listeners.emplace_back(new RustEventListener(boxed_listener));
            })
        }
    }

    /// Sets default crc32 checksum generator factory
    fn set_default_checksum_gen_factory(&mut self) {
        let options_ptr = self.inner;
        unsafe {
            cpp!([options_ptr as "rocksdb_options_t*"] {
                cast_to_options(options_ptr)->file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_set_options_from_map() {
        let options = Options::default();

        let mut db_options = HashMap::new();
        db_options.insert("max_open_files", "100");

        let mut cf_options = HashMap::new();
        cf_options.insert("level_compaction_dynamic_level_bytes", "true");

        let options = options.set_db_options_from_map(&db_options).unwrap();
        let _options = options
            .set_cf_options_from_map(
                &cf_options,
                Cache::from_string(DEFAULT_BLOCK_CACHE).unwrap(),
            )
            .unwrap();
    }

    #[test]
    #[should_panic]
    fn test_set_invalid_db_option() {
        let options = Options::default();
        let mut map = HashMap::new();

        map.insert("bogus_option", "100");

        let _options = options.set_db_options_from_map(&map).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_set_invalid_cf_option() {
        let options = Options::default();
        let mut map = HashMap::new();

        map.insert("bogus_option", "100");

        let _options = options
            .set_cf_options_from_map(&map, Cache::from_string(DEFAULT_BLOCK_CACHE).unwrap())
            .unwrap();
    }

    #[test]
    fn test_set_db_path() {
        let mut options = Options::default();
        options.set_db_path("/tmp/foo").unwrap();
    }

    #[test]
    fn test_set_cf_path() {
        let mut options = Options::default();
        options.set_cf_path("/tmp/foo").unwrap();
    }

    #[test]
    #[should_panic]
    fn test_explicitly_add_default_cf() {
        let mut options = DbOptions::default();
        options.add_column_family(DEFAULT_CF_NAME);
        options.into_components().unwrap();
    }

    #[test]
    fn test_set_block_based_options_from_map() {
        let options = BlockBasedOptions::default();
        let mut map = HashMap::new();

        map.insert("filter_policy", "bloomfilter:4:true");
        map.insert("block_cache", "1M");

        let _options = options.set_options_from_map(&map).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_set_invalid_block_based_option() {
        let options = BlockBasedOptions::default();
        let mut map = HashMap::new();

        map.insert("bogus_option", "100");

        let _options = options.set_options_from_map(&map).unwrap();
    }

    #[test]
    fn db_options_defaults_eq() {
        let options1 = DbOptions::default();
        let options2 = DbOptions::default();

        assert_eq!(options1, options2);
    }

    #[test]
    fn db_options_serde_json() -> Result<()> {
        let mut options = DbOptions::default();

        options.add_column_family("foo");
        options.add_column_family_opts("bar", &hashmap![ "baz" => "boo"]);

        let json = serde_json::to_string(&options).unwrap();
        let options2 = serde_json::from_str(&json).unwrap();

        assert_eq!(options, options2);

        // Since the merge operators can't be serialized, if we add one then we don't expect them to
        // match
        options.set_column_family_merge_operator(
            "bar",
            "my_operator",
            Arc::new(|_, _, _| None),
            None,
        )?;
        let json = serde_json::to_string(&options).unwrap();
        let options2 = serde_json::from_str(&json).unwrap();

        assert_ne!(options, options2);

        Ok(())
    }

    #[test]
    fn db_options_stats_level_default() -> Result<()> {
        let options = DbOptions::default();
        test_db_options_stats_level(options, StatsLevel::Disabled)
    }

    #[test]
    fn db_options_stats_level_full() -> Result<()> {
        let mut options = DbOptions::default();
        options.set_stats_level(StatsLevel::Full);
        test_db_options_stats_level(options, StatsLevel::Full)
    }

    #[test]
    fn db_options_stats_level_minimal() -> Result<()> {
        let mut options = DbOptions::default();
        options.set_stats_level(StatsLevel::Minimal);
        test_db_options_stats_level(options, StatsLevel::Minimal)
    }

    #[test]
    fn db_options_stats_level_disabled() -> Result<()> {
        let mut options = DbOptions::default();
        options.set_stats_level(StatsLevel::Disabled);
        test_db_options_stats_level(options, StatsLevel::Disabled)
    }

    #[test]
    fn db_options_custom_paths() -> Result<()> {
        let mut options = DbOptions::default();
        options.set_db_path("/tmp/db_path");
        options.add_column_family("foo");
        options.set_column_family_path("foo", "/tmp/db_path/foo");
        let _components = options.into_components()?;

        // Testing that these options actually get applied properly by rocks is done in the DB
        // tests, not here
        Ok(())
    }

    #[allow(clippy::transmute_num_to_bytes)]
    fn test_db_options_stats_level(options: DbOptions, level: StatsLevel) -> Result<()> {
        let DbComponents { options, .. } = options.into_components()?;

        let opts_ptr = options.inner;
        let is_enabled = level != StatsLevel::Disabled;
        let level = level.as_rocks_level().unwrap_or(0xff);
        let failure_msg = unsafe {
            cpp!([opts_ptr as "rocksdb_options_t*", is_enabled as "bool", level as "uint8_t"] -> *const libc::c_char as "const char*" {
                auto options_ptr = reinterpret_cast<rocksdb::Options*>(opts_ptr);

                if (!options_ptr) {
                    return "options_ptr is NULL";
                }

                if (!is_enabled) {
                    if (!is_enabled && options_ptr->statistics) {
                        return "options_ptr->statistics is not NULL";
                    } else {
                        return NULL;
                    }
                }

                if (!options_ptr->statistics) {
                    return "options_ptr->statistics is NULL";
                }

                if (options_ptr->statistics->get_stats_level() != level) {
                    return "options_ptr->statistics->get_stats_level() is not correct";
                }

                return NULL;
            })
        };

        if !failure_msg.is_null() {
            unsafe {
                let message = std::ffi::CStr::from_ptr(failure_msg)
                    .to_string_lossy()
                    .into_owned();
                panic!("{}", message);
            }
        }

        Ok(())
    }

    #[test]
    fn db_options_block_options_override() -> Result<()> {
        let mut options = DbOptions::default();
        options.add_column_family_opts(
            "foo",
            &hashmap!["block_based_table_factory" => "block_size=64K"],
        );
        options.add_column_family("bar");

        let default_block_opts = options.get_default_cf_block_table_options();
        let foo_block_opts = options.get_column_family_block_table_options("foo")?;
        let bar_block_opts = options.get_column_family_block_table_options("bar")?;

        assert_eq!(default_block_opts.get("block_size").unwrap(), "32K");
        assert_eq!(foo_block_opts.get("block_size").unwrap(), "64K");

        // For `bar`, no `block_size` option was set explicitly, so it will be derived from the
        // default when calling `get_default_cf_block_table_options`
        assert_eq!(
            bar_block_opts.get("block_size"),
            default_block_opts.get("block_size")
        );

        // Now build the options structs.
        //
        // The default and `bar` CFs will get the default 32K block size, while `foo` gets 64K.
        let DbComponents {
            column_families: cfs,
            ..
        } = options.into_components()?;

        // `cfs` will contain the CFs that were explicitly added and the dfault.  There is an implied `default` CF
        // also whose parameters are controlled by what's set in `options`
        assert!(cfs.iter().any(|cf| cf.name == "foo"));
        assert!(cfs.iter().any(|cf| cf.name == "bar"));
        assert!(cfs.iter().any(|cf| cf.name == DEFAULT_CF_NAME));
        assert_eq!(3, cfs.len());

        // Sadly as of the upgrade to Rocks 6.15.4 the C++ API no longer provides a way to get the
        // block size option.

        /*

        for cf in cfs.into_iter() {
            let name = cf.name.as_ref();
            let options = &cf.options;

            let block_size = get_block_size(options);
            let metadata_block_size = get_metadata_block_size(options);
            let format_version = get_format_version(options);

            // One of the CFs has a non-default block size
            let expected_block_size = match name {
                "foo" => 64 * 1024,
                _ => 32 * 1024,
            };

            // But all of the CFs should have inherited the default metadata block size
            // and format version
            let expected_metadata_block_size = 4 * 1024;
            let expected_format_version = 5;

            assert_eq!(expected_block_size, block_size, "CF name: {}", name);
            assert_eq!(expected_format_version, format_version, "CF name: {}", name);
            assert_eq!(
                expected_metadata_block_size, metadata_block_size,
                "CF name: {}",
                name
            );
        }

        // Unfortunately the rocks C FFI bindings don't provide a way to get the options passed, only to
        // set them.  So we have to reach deep down in the C++ code
        fn get_block_size(options: &Options) -> usize {
            let options_ptr = options.inner;
            unsafe {
                cpp!([options_ptr as "const rocksdb::Options*"] -> libc::size_t as "size_t" {
                auto* options = reinterpret_cast<rocksdb::BlockBasedTableOptions*>(options_ptr->table_factory->GetOptions());

                return options->block_size;
                })
            }
        }

        fn get_metadata_block_size(options: &Options) -> usize {
            let options_ptr = options.inner;
            unsafe {
                cpp!([options_ptr as "const rocksdb::Options*"] -> libc::size_t as "size_t" {
                auto* options = reinterpret_cast<rocksdb::BlockBasedTableOptions*>(options_ptr->table_factory->GetOptions());

                return options->metadata_block_size;
                })
            }
        }

        fn get_format_version(options: &Options) -> u32 {
            let options_ptr = options.inner;
            unsafe {
                cpp!([options_ptr as "const rocksdb::Options*"] -> u32 as "uint32_t" {
                auto* options = reinterpret_cast<rocksdb::BlockBasedTableOptions*>(options_ptr->table_factory->GetOptions());

                return options->format_version;
                })
            }
        }
        */

        Ok(())
    }

    #[test]
    fn options_string_round_trip() {
        let options = hashmap![ "foo" => "bar", "baz" => "boo" ];

        let options_str = DbOptions::build_options_string(options);
        let options = DbOptions::parse_options_string(options_str);

        assert_eq!(
            hashmap![ "foo".to_string() => "bar".to_string(), "baz".to_string() => "boo".to_string() ],
            options
        );
    }

    #[test]
    fn with_logger_logging_context_set() {
        use crate::logging::LoggingContext;

        let new_context = crate::labels::DatabaseLabels {
            path: "/some/dumb/path",
            db_id: "foo",
        };
        let logger = crate::logging::tests::TestLogger::new();

        let context = logger.context.clone();

        let mut options = DbOptions::default();
        options.set_logger(log::LevelFilter::Debug, logger);

        let DbComponents { mut options, .. } = options.into_components().unwrap();

        options.with_logger(|logger: Option<_>| {
            let logger =
                logger.expect("there must be a logger object since one was set with `set_logger`");
            logger.set_context(new_context.clone());
        });

        assert_eq!(*context.lock().unwrap(), new_context.fields());
    }

    /// By default, when no `block_cache` options are set and no explicit `Cache` instance is
    /// provided, a new cache is created and all CFs share that same cache.
    #[test]
    fn default_block_cache_is_shared() -> Result<()> {
        let mut options = DbOptions::default();
        options.add_column_family("foo");
        options.add_column_family("bar");

        // Now build the options structs.
        let DbComponents {
            column_families: cfs,
            ..
        } = options.into_components()?;

        // All column families should use the same block cache pointer
        let default_cf = cfs
            .iter()
            .find(|cf| cf.name == DEFAULT_CF_NAME)
            .expect("BUG: missing default CF");
        let default_cache = get_block_cache(&default_cf.options);

        let (capacity, usage) = get_cache_stats(default_cache);
        assert_eq!(0, usage); // all CFs are empty and nothing has been read to the cache should be empty
        assert_eq!(1024 * 1024 * 1024, capacity); // 1GB is the default block cache size

        for cf in cfs.into_iter() {
            let cache = get_block_cache(&cf.options);
            println!("{}: {:?}", cf.name, cache);
            assert_eq!(default_cache, cache, "CF name: {}", cf.name);
        }

        Ok(())
    }

    /// By default, if no CF gets a separate block cache config, all CFs in a DB share the same
    /// block cache.  This should be true even if other block-based table factory options are
    /// overridden at the CF level
    #[test]
    fn custom_block_config_still_uses_default_block_cache() -> Result<()> {
        let mut options = DbOptions::default();

        // `foo` has a custom block size, but because the cache setting is not modified it should
        // still share the same default cache
        options.add_column_family_opts(
            "foo",
            &hashmap!["block_based_table_factory" => "block_size=64K"],
        );

        // `bar` inherits all CF settings from default, including the default cache
        options.add_column_family("bar");

        // Now build the options structs.
        let DbComponents {
            column_families: cfs,
            ..
        } = options.into_components()?;

        // `cfs` will contain the CFs that were explicitly added and the dfault.  There is an implied `default` CF
        // also whose parameters are controlled by what's set in `options`
        assert!(cfs.iter().any(|cf| cf.name == "foo"));
        assert!(cfs.iter().any(|cf| cf.name == "bar"));
        assert!(cfs.iter().any(|cf| cf.name == DEFAULT_CF_NAME));
        assert_eq!(3, cfs.len());

        // All column families should use the same block cache pointer
        let default_cf = cfs
            .iter()
            .find(|cf| cf.name == DEFAULT_CF_NAME)
            .expect("BUG: missing default CF");
        let default_cache = get_block_cache(&default_cf.options);

        let (capacity, usage) = get_cache_stats(default_cache);
        assert_eq!(0, usage); // all CFs are empty and nothing has been read to the cache should be empty
        assert_eq!(1024 * 1024 * 1024, capacity); // 1GB is the default block cache size

        for cf in cfs.into_iter() {
            let cache = get_block_cache(&cf.options);
            println!("{}: {:?}", cf.name, cache);
            assert_eq!(default_cache, cache, "CF name: {}", cf.name);
        }

        Ok(())
    }

    /// It's possible to give a single CF its own block cache, by configuring block cache options
    /// at the CF level
    #[test]
    fn cf_with_custom_block_cache() -> Result<()> {
        let mut options = DbOptions::default();

        // foo will get the default options
        options.add_column_family("foo");

        // bar gets a custom block based table factory config
        options.add_column_family_opts(
            "bar",
            &hashmap!["block_based_table_factory" => "block_cache=512M"],
        );

        // Now build the options structs.
        let DbComponents {
            column_families: cfs,
            ..
        } = options.into_components()?;

        // `default` and `foo` should share the same cache
        let default_cf = cfs
            .iter()
            .find(|cf| cf.name == DEFAULT_CF_NAME)
            .expect("BUG: missing default CF");
        let default_cache = get_block_cache(&default_cf.options);

        let (capacity, usage) = get_cache_stats(default_cache);
        assert_eq!(0, usage); // all CFs are empty and nothing has been read to the cache should be empty
        assert_eq!(1024 * 1024 * 1024, capacity); // 1GB is the default block cache size

        let foo_cf = cfs.iter().find(|cf| cf.name == "foo").unwrap();
        let foo_cache = get_block_cache(&foo_cf.options);
        assert_eq!(default_cache, foo_cache);

        // Meanwhile `bar` should have a separate cache
        let bar_cf = cfs.iter().find(|cf| cf.name == "bar").unwrap();
        let bar_cache = get_block_cache(&bar_cf.options);
        assert_ne!(default_cache, bar_cache);

        let (capacity, usage) = get_cache_stats(bar_cache);
        assert_eq!(0, usage);
        assert_eq!(512 * 1024 * 1024, capacity);

        Ok(())
    }

    /// With a pre-existing `Cache` instance is set as the default block cache in the DB config,
    /// this cache is used by all CFs
    #[test]
    fn concrete_default_block_cache_is_shared() -> Result<()> {
        let mut options = DbOptions::default();
        options.add_column_family("foo");
        options.add_column_family("bar");

        let cache = Cache::with_capacity(10_000_000)?;

        options.set_default_block_cache(cache.clone());

        // Now build the options structs.
        let DbComponents {
            column_families: cfs,
            ..
        } = options.into_components()?;

        // All column families should use the given block cache
        for cf in cfs.into_iter() {
            let cache_ptr = get_block_cache(&cf.options);
            unsafe {
                assert_eq!(cache.rocksdb_cache_ptr(), cache_ptr, "CF name: {}", cf.name);
            }
        }

        Ok(())
    }

    /// For both default and non-default CFs, an explicit `Cache` instance always trumps a cache
    /// config on an options string
    #[test]
    fn concrete_block_cache_overrides_options_string() -> Result<()> {
        let mut options = DbOptions::default();
        // Set default table options that specify a 2MB cache, but also set a concrete `Cache`
        // instance as the default block cache.
        options.set_default_cf_block_table_options(hashmap!["block_cache" => "2MB"]);
        let default_cache = Cache::with_capacity(10_000_000)?;
        options.set_default_block_cache(default_cache.clone());

        // Add `foo` CF without any additional block table options
        options.add_column_family("foo");

        // Configure bar with a `block_cache` option, plus a separate `Cache` instance
        let bar_cache = Cache::with_capacity(5_000_000)?;
        options.add_column_family_opts(
            "bar",
            &hashmap!["block_based_table_factory" => "block_cache=512M"],
        );
        options.set_column_family_block_cache("bar", bar_cache.clone());

        // Now build the options structs.
        let DbComponents {
            column_families: cfs,
            ..
        } = options.into_components()?;

        // Both of the `block_cache` options should have been ignored, and instead the explicit
        // `Cache` objects used
        unsafe {
            let default_cf = cfs
                .iter()
                .find(|cf| cf.name == DEFAULT_CF_NAME)
                .expect("BUG: missing default CF");
            let default_cache_ptr = get_block_cache(&default_cf.options);
            assert_eq!(default_cache.rocksdb_cache_ptr(), default_cache_ptr);

            // foo should be using the default
            let foo_cf = cfs.iter().find(|cf| cf.name == "foo").unwrap();
            let foo_cache_ptr = get_block_cache(&foo_cf.options);
            assert_eq!(default_cache.rocksdb_cache_ptr(), foo_cache_ptr);

            // Meanwhile `bar` should have a separate cache, the one we explicitly created
            let bar_cf = cfs.iter().find(|cf| cf.name == "bar").unwrap();
            let bar_cache_ptr = get_block_cache(&bar_cf.options);
            assert_eq!(bar_cache.rocksdb_cache_ptr(), bar_cache_ptr);
        }

        Ok(())
    }

    #[test]
    fn parse_rocksdb_config_string() -> Result<()> {
        let opts_str = concat!(
            "write_buffer_size=1024;",
            "nested_opt={opt1=1;opt2=2};",
            "max_bytes_for_level_base=100;",
            "metadata_cache_options={",
            "top_level_index_pinning=kAll;",
            "partition_pinning=kFlushedAndSimilar;",
            "unpartitioned_pinning=kFlushedAndSimilar;",
            "};"
        );

        let map = DbOptions::parse_options_string(opts_str.to_string());
        assert_eq!(map.len(), 4);
        assert_eq!(map["write_buffer_size"], "1024");
        assert_eq!(map["nested_opt"], "opt1=1;opt2=2");
        assert_eq!(map["max_bytes_for_level_base"], "100");
        assert_eq!(map["metadata_cache_options"], "top_level_index_pinning=kAll;partition_pinning=kFlushedAndSimilar;unpartitioned_pinning=kFlushedAndSimilar;");

        Ok(())
    }

    // Unfortunately the rocks C FFI bindings don't provide a way to get the options passed, only to
    // set them.  So we have to reach deep down in the C++ code
    fn get_block_cache(options: &Options) -> *mut libc::c_void {
        let options_ptr = options.inner;
        unsafe {
            cpp!([options_ptr as "const rocksdb::Options*"] -> *mut libc::c_void as "const void*" {
                // HACK: we're just assuming that the table factory is a block-based table
                // factory.  If not...boom!
                auto* factory = reinterpret_cast<const rocksdb::BlockBasedTableFactory*>(options_ptr->table_factory.get());

                // see the implementation of this gem in db_options.h for more nauseating C++
                // debauchery
                auto* cache = HackBBTF::GetCacheFromBlockBasedTableFactory(factory);

                return cache;
            })
        }
    }

    /// Given a pointer to a RocksDB C++ Cache object, get the capacity and usage of the cache
    fn get_cache_stats(cache_ptr: *const libc::c_void) -> (isize, isize) {
        unsafe {
            let capacity = cpp!([cache_ptr as "const rocksdb::Cache*"] -> isize as "size_t" {
                return cache_ptr->GetCapacity();
            });
            let usage = cpp!([cache_ptr as "const rocksdb::Cache*"] -> isize as "size_t" {
                return cache_ptr->GetUsage();
            });

            (capacity, usage)
        }
    }
}
