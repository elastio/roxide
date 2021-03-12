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
use cheburashka::logging::prelude::*;
use maplit::hashmap;
use num_traits::PrimInt;
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use std::ffi::c_void;
use std::fmt::{self, Display};
use std::iter::FromIterator;
use std::ptr;
use std::{
    borrow::Borrow,
    path::{Path, PathBuf},
};
use std::{collections::HashMap, ffi::CString};

// Re-export the RocksDB options types for convenience, so callers don't take any direct
// dependencies on the underlying `rocksdb` crate
pub use rocksdb::{
    self, BlockBasedIndexType, BlockBasedOptions, DBCompactionStyle, DBCompressionType, DBPath,
    DBRecoveryMode, FlushOptions, MemtableFactory, Options, PlainTableFactoryOptions, ReadOptions,
    WriteOptions,
};

pub mod prelude {
    // Most code will want all of our extension traits on the RocksDB options types
    pub use super::{BlockBasedOptionsExt, ColumnFamilyDescriptor, DBOptions, OptionsExt};

    // And also needs the RocksDB option types themselves, which we re-export to make them look
    // like our own
    pub use super::{
        BlockBasedIndexType, BlockBasedOptions, DBCompactionStyle, DBCompressionType,
        DBRecoveryMode, FlushOptions, MemtableFactory, Options, PlainTableFactoryOptions,
        ReadOptions, WriteOptions,
    };
}

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

/// All databases always have a column family by this name, whether it's specified in the options or
/// not.
pub const DEFAULT_CF_NAME: &str = "default";

/// A descriptor for a RocksDB column family.
///
/// A description of the column family, containing the name and `Options`.
pub struct ColumnFamilyDescriptor {
    pub(crate) name: String,
    pub(crate) options: Options,
}

impl ColumnFamilyDescriptor {
    /// Create a new column family descriptor with the specified name and options.
    pub fn new<S>(name: S, options: Options) -> Self
    where
        S: Into<String>,
    {
        ColumnFamilyDescriptor {
            name: name.into(),
            options,
        }
    }

    /// The name of this column family.
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Struct that contains all of the information about a database other than it's path.  Not to be
/// confused with the `rocksdb::DBOptions` struct which is something entirely different and more
/// confusing
///
/// This struct is used to build `rocksdb::Options` (which wraps the C++ `Options` struct) and
/// `rocksdb::ColumnFamilyDescriptor` (which wraps the `Options` and name for each column family).
#[derive(Serialize, Deserialize)]
pub struct DBOptions {
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

    /// Additional (non-default) column families.  The key is the name of the CF, and the value is
    /// a `HashMap` of settings for that CF.  Each CF inherits settings from `default_cf_options`
    /// unless they are explicitly overridden.
    column_families: HashMap<String, HashMap<String, String>>,

    /// Custom database paths per column family.  The key is the same as `column_families`: the
    /// name of the CF.  If a given column family doesn't have a path in this field, then `db_path`
    /// is used; if `db_path` is `None`, then whatever path was passed to `open*` is used.
    column_family_paths: HashMap<String, PathBuf>,

    stats_level: StatsLevel,

    /// The merge operators, indexed by column family name
    ///
    /// This field cannot be serialized since `Fn` types are not generally speaking serializable.
    /// So callers must populate the merge operators explicitly even if the rest of the options are
    /// loaded from a config file
    #[serde(skip)]
    merge_operators: HashMap<String, (String, merge::MergeFn, Option<merge::MergeFn>)>,

    /// An optional event listener which will receive various RocksDB notifications
    #[serde(skip)]
    event_listener: Option<Box<dyn events::RocksDbEventListener>>,

    /// An optional logger which will be passed all RocksDB log messages at or above `log_level`
    #[serde(skip)]
    logger: Option<Box<dyn logging::RocksDbLogger>>,

    /// The RocksDB internal log level, controlling what level of log messages get passed to
    /// `logger`.  If `logger` is `None`, this option has no effect
    #[serde(skip, default = "log::LevelFilter::max")]
    log_level: log::LevelFilter,
}

impl DBOptions {
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
        DBOptions {
            db_options: Self::make_owned_hash_map(db_options),

            db_path: None,

            default_cf_options: Self::make_owned_hash_map(default_cf_options),

            column_families: HashMap::from_iter(
                column_families
                    .borrow()
                    .iter()
                    .map(|(k, v)| (k.to_string(), Self::make_owned_hash_map(v))),
            ),

            column_family_paths: HashMap::new(),

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
    pub fn get_default_cf_block_table_options(&self) -> Result<HashMap<String, String>> {
        if let Some(opts_str) = self.default_cf_options.get("block_based_table_factory") {
            Self::parse_options_string(opts_str)
        } else {
            // No such options
            Ok(HashMap::new())
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

    /// Sets the default number of bits per key to use for creation of bloom filters for all column
    /// families.
    ///
    /// Applies only if the default block-based table factory is used.
    pub fn set_default_cf_block_bloom_filter_bits(&mut self, bits_per_key: usize) -> Result<()> {
        let mut options = self.get_default_cf_block_table_options()?;
        options.insert(
            "filter_policy".to_string(),
            format!("bloomfilter:{}:false", bits_per_key),
        );

        self.set_default_cf_block_table_options(options);

        Ok(())
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
                Self::parse_options_string(opts_str)
            } else {
                // No such options
                self.get_default_cf_block_table_options()
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
            cf_opts.insert(
                "block_based_table_factory".to_string(),
                Self::build_options_string(options),
            );

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
        full_merge_fn: merge::MergeFn,
        partial_merge_fn: Option<merge::MergeFn>,
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
        self.event_listener = Some(Box::new(listener));
    }

    /// Sets the logger which will receive all of the RocksDB log messages corresponding to `level`
    pub fn set_logger(
        &mut self,
        level: log::LevelFilter,
        logger: impl logging::RocksDbLogger + 'static,
    ) {
        self.logger = Some(Box::new(logger));
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
            self.logger = Some(Box::new(logger));
            self.log_level = level;
        }
    }

    /// Once the options are set correctly, this method is called to consume the struct and convert
    /// it into a `Options` struct with the database options, and a vector of
    /// `ColumnFamilyDescriptor` struct with the name and options of each column family.  This is
    /// the representation which RocksDB needs to operate on.
    pub fn into_components(mut self) -> Result<(Options, Vec<ColumnFamilyDescriptor>)> {
        let opts = Options::default();

        // Apply the DB and defualt CF options directly to this `Options` struct
        let default_block_options = self.get_default_cf_block_table_options()?;
        let mut opts = opts.set_db_options_from_map(&self.db_options)?;
        if let Some(db_path) = self.db_path.as_ref() {
            opts.set_db_path(db_path)?;
        }
        let mut opts = opts.set_cf_options_from_map(&self.default_cf_options)?;
        if let Some(logger) = self.logger {
            if let Some(level) = self.log_level.to_level() {
                opts.set_logger(level, logger);
            }
        }

        if let Some(event_listener) = self.event_listener {
            opts.set_event_listener(event_listener);
        }

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

        let merge_operators = self.merge_operators;
        let column_family_paths = self.column_family_paths;

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

        // Create `ColumnFamilyDescriptor`s for every additional column family
        // If there were any errors, return the first one
        let cfs: Result<Vec<(String, Options)>, Error> = self
            .column_families
            .into_iter()
            .map(|(name, options)| {
                let mut options = options;

                // Take special care with `block_based_table_factory`, which unlike the other
                // options we'll actually parse out of both the default and the CF-specific options
                // hashes and use the combination of both.
                if let Some(cf_block_options) = options.get("block_based_table_factory") {
                    let mut cf_block_options = Self::parse_options_string(cf_block_options)?;
                    for (key, value) in default_block_options.iter() {
                        if !cf_block_options.contains_key(key) {
                            cf_block_options.insert(key.to_string(), value.to_string());
                        }
                    }

                    if !cf_block_options.is_empty() {
                        options.insert(
                            "block_based_table_factory".to_string(),
                            Self::build_options_string(&cf_block_options),
                        );
                    }
                }

                // Start from the default CF options, but layer on this family-specific options
                let mut cf_opts = opts.set_cf_options_from_map(&options)?;

                // If there's a merge operator for this CF, apply that now
                if let Some((operator_name, full, partial)) = merge_operators.get(&name) {
                    cf_opts.set_merge_operator(&operator_name, *full, *partial);
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
            .map(|(name, options)| ColumnFamilyDescriptor::new(name, options))
            .collect();

        Ok((opts, cf_descriptors))
    }

    /// Given some options in the RocksDB options string format, parses the options into a hash
    /// table.
    fn parse_options_string(opts: impl AsRef<str>) -> Result<HashMap<String, String>> {
        let pairs = opts.as_ref().split(';');

        let pairs = pairs.filter_map(|pair| {
            let pair = pair.trim();

            if !pair.is_empty() {
                let pair: Vec<_> = pair.split('=').collect();

                Some((pair[0].trim().to_string(), pair[1].trim().to_string()))
            } else {
                None
            }
        });

        Ok(HashMap::from_iter(pairs))
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
            opts_str.push_str(&format!("{}={};", key.to_string(), value.to_string()));
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
        HashMap::from_iter(
            map.borrow()
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string())),
        )
    }
}

impl fmt::Debug for DBOptions {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "DBOptions(
            db_options: {:?},
            default_cf_options: {:?},
            column_families: {:?},
            stats_level: {:?},
            merge_operators: {:?},
            )",
            self.db_options,
            self.default_cf_options,
            self.column_families,
            self.stats_level,
            self.merge_operators
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
                .collect::<Vec<_>>()
        )
    }
}

impl PartialEq for DBOptions {
    fn eq(&self, other: &Self) -> bool {
        self.db_options == other.db_options
            && self.default_cf_options == other.default_cf_options
            && self.column_families == other.column_families
            && self.stats_level == other.stats_level
            && self.merge_operators.keys().collect::<Vec<_>>()
                == other.merge_operators.keys().collect::<Vec<_>>()
    }
}

impl Default for DBOptions {
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

                // Similar to `pin_l0...` except `pin_l0..` pins the entire filter/index
                // blocks for L0 data files.  This pins the top-level (of the two levels
                // enabled by `index_type`) index and filter _OF EACH SST_, on the theory
                // that the top level indexes are very small, and having them always in
                // cache lets us very quickly identify which partition(s) in a given SST
                // may contain the data we need.  The second level index/filter blocks are also
                // cached per `cache_index_and_filter_blocks`, but those blocks may or may not be
                // pushed out of the cache depending on cache pressure.
                "pin_top_level_index_and_filter=true;",

                // Pin the L0 filterand index blocks in the cache so they are never pushed out by
                // data blocks
                "pin_l0_filter_and_index_blocks_in_cache=true;",

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

        DBOptions::new(db_opts, default_cf_opts, HashMap::new())
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

    let err = setter_func(&options, &new_options, map_ptr);

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
                        auto status = rocksdb::GetBlockBasedTableOptionsFromMap(
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
    /// # Example
    ///
    /// ```
    /// use std::collections::HashMap;
    /// use roxide::{Options, OptionsExt};
    ///
    /// let mut options = HashMap::new();
    /// options.insert("report_bg_io_stats", "true");
    /// options.insert("max_compaction_bytes", "1M");
    /// let opt = Options::default();
    /// let _opt = opt.set_cf_options_from_map(&options).unwrap();
    /// ```
    fn set_cf_options_from_map<K: AsRef<str> + Into<Vec<u8>>, V: AsRef<str> + Into<Vec<u8>>>(
        &self,
        map: &HashMap<K, V>,
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
    fn set_logger(&mut self, min_level: log::Level, logger: Box<dyn logging::RocksDbLogger>);

    fn inner_logger(&self) -> *mut ffi::rocksdb_options_t;

    /// If there is a C++ logger configured, temporarily converts it back to a Rust impl and passes
    /// that impl to a closure for additional customization.
    ///
    /// If there is no logger configured, `None` is passed to the closure.
    fn with_logger<R, F: FnOnce(Option<&Box<dyn logging::RocksDbLogger>>) -> R>(
        &mut self,
        func: F,
    ) -> R
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
    fn set_event_listener(&mut self, listener: Box<dyn events::RocksDbEventListener>);
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

                    auto status = rocksdb::GetDBOptionsFromMap(
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

    fn set_cf_options_from_map<K: AsRef<str> + Into<Vec<u8>>, V: AsRef<str> + Into<Vec<u8>>>(
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
                    const rocksdb::DBOptions& existing_db_options = *static_cast<const rocksdb::DBOptions*>(options_class_ptr);
                    auto new_cf_options = rocksdb::ColumnFamilyOptions();

                    auto status = rocksdb::GetColumnFamilyOptionsFromMap(
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
        let rocks_path = DBPath::new(path, 0).with_context(|| crate::error::RustRocksDBError {})?;
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

    fn set_logger(&mut self, min_level: log::Level, logger: Box<dyn logging::RocksDbLogger>) {
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
    fn set_event_listener(&mut self, listener: Box<dyn events::RocksDbEventListener>) {
        let listener = events::CppListenerWrapper::wrap(listener);

        let options_ptr = self.inner;
        let boxed_listener = listener.into_raw_void();

        unsafe {
            cpp!([options_ptr as "rocksdb_options_t*", boxed_listener as "void*"] {
                cast_to_options(options_ptr)->listeners.emplace_back(new RustEventListener(boxed_listener));
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
        let _options = options.set_cf_options_from_map(&cf_options).unwrap();
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

        let _options = options.set_cf_options_from_map(&map).unwrap();
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
        let mut options = DBOptions::default();
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
        let options1 = DBOptions::default();
        let options2 = DBOptions::default();

        assert_eq!(options1, options2);
    }

    #[test]
    fn db_options_serde_json() -> Result<()> {
        let mut options = DBOptions::default();

        options.add_column_family("foo");
        options.add_column_family_opts("bar", &hashmap![ "baz" => "boo"]);

        let json = serde_json::to_string(&options).unwrap();
        let options2 = serde_json::from_str(&json).unwrap();

        assert_eq!(options, options2);

        // Since the merge operators can't be serialized, if we add one then we don't expect them to
        // match
        options.set_column_family_merge_operator("bar", "my_operator", |_, _, _| None, None)?;
        let json = serde_json::to_string(&options).unwrap();
        let options2 = serde_json::from_str(&json).unwrap();

        assert_ne!(options, options2);

        Ok(())
    }

    #[test]
    fn db_options_stats_level_default() -> Result<()> {
        let options = DBOptions::default();
        test_db_options_stats_level(options, StatsLevel::Disabled)
    }

    #[test]
    fn db_options_stats_level_full() -> Result<()> {
        let mut options = DBOptions::default();
        options.set_stats_level(StatsLevel::Full);
        test_db_options_stats_level(options, StatsLevel::Full)
    }

    #[test]
    fn db_options_stats_level_minimal() -> Result<()> {
        let mut options = DBOptions::default();
        options.set_stats_level(StatsLevel::Minimal);
        test_db_options_stats_level(options, StatsLevel::Minimal)
    }

    #[test]
    fn db_options_stats_level_disabled() -> Result<()> {
        let mut options = DBOptions::default();
        options.set_stats_level(StatsLevel::Disabled);
        test_db_options_stats_level(options, StatsLevel::Disabled)
    }

    #[test]
    fn db_options_custom_paths() -> Result<()> {
        let mut options = DBOptions::default();
        options.set_db_path("/tmp/db_path");
        options.add_column_family("foo");
        options.set_column_family_path("foo", "/tmp/db_path/foo");
        let (_options, _cfs) = options.into_components()?;

        // Testing that these options actually get applied properly by rocks is done in the DB
        // tests, not here
        Ok(())
    }

    fn test_db_options_stats_level(options: DBOptions, level: StatsLevel) -> Result<()> {
        let (options, _cfs) = options.into_components()?;

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
        let mut options = DBOptions::default();
        options.add_column_family_opts(
            "foo",
            &hashmap!["block_based_table_factory" => "block_size=64K"],
        );
        options.add_column_family("bar");

        let default_block_opts = options.get_default_cf_block_table_options()?;
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
        let (_options, cfs) = options.into_components()?;

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

        let options_str = DBOptions::build_options_string(options);
        let options = DBOptions::parse_options_string(options_str).unwrap();

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

        let mut options = DBOptions::default();
        options.set_logger(log::LevelFilter::Debug, logger);

        let (mut options, _) = options.into_components().unwrap();

        options.with_logger(|logger: Option<_>| {
            let logger =
                logger.expect("there must be a logger object since one was set with `set_logger`");
            logger.set_context(new_context.clone());
        });

        assert_eq!(*context.lock().unwrap(), new_context.fields());
    }
}
