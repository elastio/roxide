//! The `GetProperty` operation reads named properties from the database to get various information
//! about the internal state of the database engine.
//!
//! RocksDB properties are either strings, ints, or maps of name/value pairs.  This module adjusts
//! some of those types to be more usable from Rust, but in principle preserves the same concept.
use super::op_metrics;
use super::*;
use crate::db;
use crate::ops::get_db_ptr::GetDbPtr;
use crate::{Error, Result};

use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;
use std::str::FromStr;
use strum::{Display, EnumIter, EnumProperty};

cpp! {{
    #include "src/ops/get_prop.h"
    #include "src/ops/get_prop.cpp"
}}

type LevelNumber = u8;

/// Contains one member for every possible RocksDB property.
///
/// This enum is generated and maintained by hand based on the RocksDB source code file
/// `db/internal_stats.cc`.  The comments before each property are the C++ source code where the
/// constant for that property is defined
#[derive(Copy, Clone, Display, EnumIter, EnumProperty, Hash, PartialEq, Eq)]
pub enum RocksProperty {
    // static const std::string num_files_at_level_prefix = "num-files-at-level";
    #[strum(props(type = "string", parse_as = "int", name = "rocksdb.num-files-at-level"))]
    NumFilesAtLevel(LevelNumber),

    // static const std::string compression_ratio_at_level_prefix =
    //    "compression-ratio-at-level";
    #[strum(props(
        type = "string",
        parse_as = "double",
        name = "rocksdb.compression-ratio-at-level"
    ))]
    CompressionRatioAtLevel(LevelNumber),

    // static const std::string allstats = "stats";
    #[strum(props(type = "string", name = "rocksdb.stats"))]
    AllStats,

    // static const std::string sstables = "sstables";
    #[strum(props(type = "string", name = "rocksdb.sstables"))]
    SsTables,

    // static const std::string cfstats = "cfstats";
    #[strum(props(type = "map", name = "rocksdb.cfstats"))]
    ColumnFamilyStats,

    // static const std::string cfstats_no_file_histogram =
    //    "cfstats-no-file-histogram";
    #[strum(props(type = "string", name = "rocksdb.cfstats-no-file-histogram"))]
    CfStatsNoFileHistogram,

    // static const std::string cf_file_histogram = "cf-file-histogram";
    #[strum(props(type = "string", name = "rocksdb.cf-file-histogram"))]
    CfFileHistogram,

    // static const std::string dbstats = "dbstats";
    #[strum(props(type = "string", name = "rocksdb.dbstats"))]
    DbStats,

    // static const std::string levelstats = "levelstats";
    #[strum(props(type = "string", name = "rocksdb.levelstats"))]
    LevelStats,

    // static const std::string num_immutable_mem_table = "num-immutable-mem-table";
    #[strum(props(type = "int", name = "rocksdb.num-immutable-mem-table"))]
    NumImmutableMemTable,

    // static const std::string num_immutable_mem_table_flushed =
    //    "num-immutable-mem-table-flushed";
    #[strum(props(type = "int", name = "rocksdb.num-immutable-mem-table-flushed"))]
    NumImmutableMemTableFlushed,

    // static const std::string mem_table_flush_pending = "mem-table-flush-pending";
    #[strum(props(type = "int", name = "rocksdb.mem-table-flush-pending"))]
    MemTableFlushPending,

    // static const std::string compaction_pending = "compaction-pending";
    #[strum(props(type = "int", name = "rocksdb.compaction-pending"))]
    CompactionPending,

    // static const std::string background_errors = "background-errors";
    #[strum(props(type = "int", name = "rocksdb.background-errors"))]
    BackgroundErrors,

    // static const std::string cur_size_active_mem_table =
    //    "cur-size-active-mem-table";
    #[strum(props(type = "int", name = "rocksdb.cur-size-active-mem-table"))]
    CurrentSizeActiveMemTable,

    // static const std::string cur_size_all_mem_tables = "cur-size-all-mem-tables";
    #[strum(props(type = "int", name = "rocksdb.cur-size-all-mem-tables"))]
    CurrentSizeAllMemTables,

    // static const std::string size_all_mem_tables = "size-all-mem-tables";
    #[strum(props(type = "int", name = "rocksdb.size-all-mem-tables"))]
    SizeAllMemTables,

    // static const std::string num_entries_active_mem_table =
    //    "num-entries-active-mem-table";
    #[strum(props(type = "int", name = "rocksdb.num-entries-active-mem-table"))]
    NumEntriesActiveMemTable,

    // static const std::string num_entries_imm_mem_tables =
    //    "num-entries-imm-mem-tables";
    #[strum(props(type = "int", name = "rocksdb.num-entries-imm-mem-tables"))]
    NumEntriesImmutableMemTables,

    // static const std::string num_deletes_active_mem_table =
    //    "num-deletes-active-mem-table";
    #[strum(props(type = "int", name = "rocksdb.num-deletes-active-mem-table"))]
    NumDeletesActiveMemtable,

    // static const std::string num_deletes_imm_mem_tables =
    //    "num-deletes-imm-mem-tables";
    #[strum(props(type = "int", name = "rocksdb.num-deletes-active-mem-table"))]
    NumDeletesImmutableMemtables,

    // static const std::string estimate_num_keys = "estimate-num-keys";
    #[strum(props(type = "int", name = "rocksdb.estimate-num-keys"))]
    EstimateNumberOfKeys,

    // static const std::string estimate_table_readers_mem =
    //    "estimate-table-readers-mem";
    #[strum(props(type = "int", name = "rocksdb.estimate-table-readers-mem"))]
    EstimateTableReadersMem,

    // static const std::string is_file_deletions_enabled =
    //    "is-file-deletions-enabled";
    #[strum(props(type = "int", name = "rocksdb.is-file-deletions-enabled"))]
    IsFileDeletionsEnabled,

    // static const std::string num_snapshots = "num-snapshots";
    #[strum(props(type = "int", name = "rocksdb.num-snapshots"))]
    NumSnapshots,

    // static const std::string oldest_snapshot_time = "oldest-snapshot-time";
    #[strum(props(type = "int", name = "rocksdb.oldest-snapshot-time"))]
    OldestSnapshotTime,

    // static const std::string num_live_versions = "num-live-versions";
    #[strum(props(type = "int", name = "rocksdb.num-live-versions"))]
    NumLiveVersions,

    // static const std::string current_version_number =
    //    "current-super-version-number";
    #[strum(props(type = "int", name = "rocksdb.current-super-version-number"))]
    CurrentVersionNumber,

    // static const std::string estimate_live_data_size = "estimate-live-data-size";
    #[strum(props(type = "int", name = "rocksdb.estimate-live-data-size"))]
    EstimateLiveDataSize,

    // static const std::string min_log_number_to_keep_str = "min-log-number-to-keep";
    #[strum(props(type = "int", name = "rocksdb.min-log-number-to-keep"))]
    MinLogNumberToKeep,

    // static const std::string min_obsolete_sst_number_to_keep_str =
    //    "min-obsolete-sst-number-to-keep";
    #[strum(props(type = "int", name = "rocksdb.min-obsolete-sst-number-to-keep"))]
    MinObsoleteSstNumberToKeep,

    // static const std::string base_level_str = "base-level";
    #[strum(props(type = "int", name = "rocksdb.base-level"))]
    BaseLevel,

    // static const std::string total_sst_files_size = "total-sst-files-size";
    #[strum(props(type = "int", name = "rocksdb.total-sst-files-size"))]
    TotalSstFilesSize,

    // static const std::string live_sst_files_size = "live-sst-files-size";
    #[strum(props(type = "int", name = "rocksdb.live-sst-files-size"))]
    LiveSstFilesSize,

    // static const std::string estimate_pending_comp_bytes =
    //    "estimate-pending-compaction-bytes";
    #[strum(props(type = "int", name = "rocksdb.estimate-pending-compaction-bytes"))]
    EstimatePendingCompactionBytes,

    // static const std::string aggregated_table_properties =
    //    "aggregated-table-properties";
    #[strum(props(type = "string", name = "rocksdb.aggregated-table-properties"))]
    AggregatedTableProperties,

    // static const std::string aggregated_table_properties_at_level =
    //    aggregated_table_properties + "-at-level";
    #[strum(props(type = "string", name = "rocksdb.aggregated-table-properties-at-level"))]
    AggregatedTablePropertiesAtLevel(LevelNumber),

    // static const std::string num_running_compactions = "num-running-compactions";
    #[strum(props(type = "int", name = "rocksdb.num-running-compactions"))]
    NumRunningCompactions,

    // static const std::string num_running_flushes = "num-running-flushes";
    #[strum(props(type = "int", name = "rocksdb.num-running-flushes"))]
    NumRunningFlushes,

    // static const std::string actual_delayed_write_rate =
    //    "actual-delayed-write-rate";
    #[strum(props(type = "int", name = "rocksdb.actual-delayed-write-rate"))]
    ActualDelayedWriteRate,

    // static const std::string is_write_stopped = "is-write-stopped";
    #[strum(props(type = "int", name = "rocksdb.is-write-stopped"))]
    IsWriteStopped,

    // static const std::string estimate_oldest_key_time = "estimate-oldest-key-time";
    // According to Rocks docs this is only available for FIFO compaction with allow_compaction =
    // false.  That never applies to us.
    // #[strum(props(type = "int", name = "rocksdb.estimate-oldest-key-time"))]
    // EstimateOldestKeyTime,

    // static const std::string block_cache_capacity = "block-cache-capacity";
    #[strum(props(type = "int", name = "rocksdb.block-cache-capacity"))]
    BlockCacheCapacity,

    // static const std::string block_cache_usage = "block-cache-usage";
    #[strum(props(type = "int", name = "rocksdb.block-cache-usage"))]
    BlockCacheUsage,

    // static const std::string block_cache_pinned_usage = "block-cache-pinned-usage";
    #[strum(props(type = "int", name = "rocksdb.block-cache-pinned-usage"))]
    BlockCachePinnedUsage,
    // static const std::string options_statistics = "options-statistics";
    // The `stats` module exposes all statistics as a Rust data structure; the properties mechanism
    // should not be used for getting stats
    // #[strum(props(type = "string", name = "rocksdb.options-statistics"))]
    // OptionsStatistics,
}

impl RocksProperty {
    /// Returns a vector of all possible properties, based on the number of levels in a particular
    /// DB
    fn all(num_levels: LevelNumber) -> Vec<RocksProperty> {
        use RocksProperty::*;
        // First make a vector of all of the properties that are not dependent upon how many levels
        // there are
        let mut properties = vec![
            AllStats,
            SsTables,
            ColumnFamilyStats,
            CfStatsNoFileHistogram,
            CfFileHistogram,
            DbStats,
            LevelStats,
            NumImmutableMemTable,
            NumImmutableMemTableFlushed,
            MemTableFlushPending,
            CompactionPending,
            BackgroundErrors,
            CurrentSizeActiveMemTable,
            CurrentSizeAllMemTables,
            SizeAllMemTables,
            NumEntriesActiveMemTable,
            NumEntriesImmutableMemTables,
            NumDeletesActiveMemtable,
            NumDeletesImmutableMemtables,
            EstimateNumberOfKeys,
            EstimateTableReadersMem,
            IsFileDeletionsEnabled,
            NumSnapshots,
            OldestSnapshotTime,
            NumLiveVersions,
            CurrentVersionNumber,
            EstimateLiveDataSize,
            MinLogNumberToKeep,
            MinObsoleteSstNumberToKeep,
            BaseLevel,
            TotalSstFilesSize,
            LiveSstFilesSize,
            EstimatePendingCompactionBytes,
            AggregatedTableProperties,
            NumRunningCompactions,
            NumRunningFlushes,
            ActualDelayedWriteRate,
            IsWriteStopped,
            BlockCacheCapacity,
            BlockCacheUsage,
            BlockCachePinnedUsage,
        ];

        properties.reserve(num_levels as usize * 3);

        for level in 0..num_levels {
            properties.extend(vec![
                NumFilesAtLevel(level),
                CompressionRatioAtLevel(level),
                AggregatedTablePropertiesAtLevel(level),
            ]);
        }

        properties
    }

    /// Gets the name of this property used by RocksDB
    pub fn name(self) -> Cow<'static, str> {
        use RocksProperty::*;

        let prop_name = self
            .get_str("name")
            .expect("All properties are expected to have a `name` attribute");

        match self {
            NumFilesAtLevel(level)
            | CompressionRatioAtLevel(level)
            | AggregatedTablePropertiesAtLevel(level) => {
                // Apply the level suffix to the name
                Cow::Owned(format!("{}{}", prop_name, level))
            }
            _ => {
                // All other properties have a name that is not parameterized by the variant's members
                Cow::Borrowed(prop_name)
            }
        }
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum RocksPropertyValue {
    StringValue(String),
    IntValue(u64),
    DoubleValue(f64),
    MapValue(HashMap<String, String>),
}

impl RocksPropertyValue {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            RocksPropertyValue::StringValue(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            RocksPropertyValue::IntValue(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            RocksPropertyValue::DoubleValue(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_map(&self) -> Option<&HashMap<String, String>> {
        match self {
            RocksPropertyValue::MapValue(v) => Some(v),
            _ => None,
        }
    }
}

fn get_property_value(
    property: RocksProperty,
    db_ptr: *mut libc::c_void,
    cf: &impl db::ColumnFamilyLike,
) -> Option<RocksPropertyValue> {
    let prop_type = property
        .get_str("type")
        .expect("All properties are expected to have a `type` attribute");

    match prop_type {
        "string" => get_property_string_value(property, db_ptr, cf),

        "int" => get_property_int_value(property, db_ptr, cf),

        "map" => get_property_map_value(property, db_ptr, cf),

        _ => unreachable!(),
    }
}

fn get_property_string_value(
    property: RocksProperty,
    db_ptr: *mut libc::c_void,
    cf: &impl db::ColumnFamilyLike,
) -> Option<RocksPropertyValue> {
    let cf_ptr = unsafe { cf.cf_ptr() };
    let prop_name = property.name();
    let prop_name_len = prop_name.as_ref().len();
    let prop_name_ptr = prop_name.as_ref().as_ptr();
    let mut string_value = String::new();

    let found = unsafe {
        let string_ptr: *mut String = &mut string_value;

        cpp!([db_ptr as "rocksdb::DB*", cf_ptr as "rocksdb::ColumnFamilyHandle*", prop_name_ptr as "const char*", prop_name_len as "size_t", string_ptr as "void*"] -> bool as "bool" {
            auto prop_name = string_as_slice(prop_name_ptr, prop_name_len);
            std::string value;

            if (get_string_property(db_ptr, cf_ptr, prop_name, value)) {
                // Success.  Convert to Rust
                auto value_ptr = value.c_str();

                rust!(GetProp_get_string [value_ptr: *const libc::c_char as "const char*", string_ptr: *mut String as "void*"] {
                    let value = std::ffi::CStr::from_ptr(value_ptr).to_string_lossy();

                    (*string_ptr).push_str(value.as_ref());
                });

                return true;
            } else {
                // Property not found
                return false;
            }
        })
    };

    if found {
        // In some cases Rocks exposes a property as a string but the value is always a
        // string representation of an integer so we hide that stupidity
        match property.get_str("parse_as") {
            None => {
                // By far the most common; this is just a string
                Some(RocksPropertyValue::StringValue(string_value))
            }
            Some("int") => {
                // Convert to an int before returning
                match u64::from_str_radix(&string_value, 10) {
                    Ok(int_value) => Some(RocksPropertyValue::IntValue(int_value)),
                    Err(_) => {
                        // Weird.  Fall back to a string value
                        Some(RocksPropertyValue::StringValue(string_value))
                    }
                }
            }
            Some("double") => {
                // Convert to an double before returning
                match f64::from_str(&string_value) {
                    Ok(double_value) => Some(RocksPropertyValue::DoubleValue(double_value)),
                    Err(_) => {
                        // Weird.  Fall back to a string value
                        Some(RocksPropertyValue::StringValue(string_value))
                    }
                }
            }
            _ => {
                // No other `parse_as` value is supported
                unreachable!()
            }
        }
    } else {
        None
    }
}

fn get_property_int_value(
    property: RocksProperty,
    db_ptr: *mut libc::c_void,
    cf: &impl db::ColumnFamilyLike,
) -> Option<RocksPropertyValue> {
    let cf_ptr = unsafe { cf.cf_ptr() };
    let prop_name = property.name();
    let prop_name_len = prop_name.as_ref().len();
    let prop_name_ptr = prop_name.as_ref().as_ptr();
    let mut int_value = 0u64;

    let found = unsafe {
        let int_ptr: *mut u64 = &mut int_value;

        cpp!([db_ptr as "rocksdb::DB*", cf_ptr as "rocksdb::ColumnFamilyHandle*", prop_name_ptr as "const char*", prop_name_len as "size_t", int_ptr as "uint64_t*"] -> bool as "bool" {
            auto prop_name = string_as_slice(prop_name_ptr, prop_name_len);
            uint64_t value;

            if (get_int_property(db_ptr, cf_ptr, prop_name, value)) {
                // Success.  Convert to Rust
                *int_ptr = value;
                return true;
            } else {
                // Property not found
                return false;
            }
        })
    };

    if found {
        Some(RocksPropertyValue::IntValue(int_value))
    } else {
        None
    }
}

fn get_property_map_value(
    property: RocksProperty,
    db_ptr: *mut libc::c_void,
    cf: &impl db::ColumnFamilyLike,
) -> Option<RocksPropertyValue> {
    let cf_ptr = unsafe { cf.cf_ptr() };
    let prop_name = property.name();
    let prop_name_len = prop_name.as_ref().len();
    let prop_name_ptr = prop_name.as_ref().as_ptr();

    let mut map_value = HashMap::<String, String>::new();
    let found = unsafe {
        let map_ptr: *mut HashMap<_, _> = &mut map_value;

        cpp!([db_ptr as "rocksdb::DB*", cf_ptr as "rocksdb::ColumnFamilyHandle*", prop_name_ptr as "const char*", prop_name_len as "size_t", map_ptr as "void*"] -> bool as "bool" {
            auto prop_name = string_as_slice(prop_name_ptr, prop_name_len);
            MapValue value;

            if (get_map_property(db_ptr, cf_ptr, prop_name, value)) {
                // Success.  Convert to Rust
                for (auto kv : value) {
                    const char* key = kv.first.c_str();
                    const char* value = kv.second.c_str();

                    rust!(GetProp_add_to_map [map_ptr: *mut HashMap<String, String> as "void*", key: *const libc::c_char as "const char*", value: *const libc::c_char as "const char*"] {
                        unsafe {
                            let key = std::ffi::CStr::from_ptr(key).to_string_lossy().into_owned();
                            let value = std::ffi::CStr::from_ptr(value).to_string_lossy().into_owned();
                            (*map_ptr).insert(key, value);
                        }
                    });
                }

                return true;
            } else {
                // Property not found
                return false;
            }
        })
    };

    if found {
        Some(RocksPropertyValue::MapValue(map_value))
    } else {
        None
    }
}

pub trait GetProperty: RocksOpBase {
    /// Gets the number of levels in this database.  This pertains to the properties because some
    /// properties are indexed by level number, starting from 0.
    fn get_number_of_levels(&self, cf: &impl db::ColumnFamilyLike) -> LevelNumber;

    /// Gets a vector of all possible properties for this column family.
    fn all_properties(&self, cf: &impl db::ColumnFamilyLike) -> Vec<RocksProperty> {
        RocksProperty::all(self.get_number_of_levels(cf))
    }

    /// Gets a database property for column family `cf`.  Properties in general are not conditional
    /// so they should always be present, but if the property is not found an error is returned.
    fn get_property(
        &self,
        cf: &impl db::ColumnFamilyLike,
        property: RocksProperty,
    ) -> Result<RocksPropertyValue>;

    /// Gets a database property as a string value.  If the property isn't a string value an error
    /// is returned.
    fn get_string_property(
        &self,
        cf: &impl db::ColumnFamilyLike,
        property: RocksProperty,
    ) -> Result<String> {
        self.get_property(cf, property)
            .and_then(|value| match value {
                RocksPropertyValue::StringValue(s) => Ok(s),
                _ => Err(Error::MismatchedPropertyTypeError {
                    property_name: property.name().to_string(),
                    property_value: format!("{:?}", value),
                }),
            })
    }

    /// Gets a database property as a int value.  If the property isn't a int value an error
    /// is returned.
    fn get_int_property(
        &self,
        cf: &impl db::ColumnFamilyLike,
        property: RocksProperty,
    ) -> Result<u64> {
        self.get_property(cf, property)
            .and_then(|value| match value {
                RocksPropertyValue::IntValue(i) => Ok(i),
                _ => Err(Error::MismatchedPropertyTypeError {
                    property_name: property.name().to_string(),
                    property_value: format!("{:?}", value),
                }),
            })
    }

    /// Gets a database property as a map value.  If the property isn't a map value an error
    /// is returned.
    fn get_map_property(
        &self,
        cf: &impl db::ColumnFamilyLike,
        property: RocksProperty,
    ) -> Result<HashMap<String, String>> {
        self.get_property(cf, property)
            .and_then(|value| match value {
                RocksPropertyValue::MapValue(m) => Ok(m),
                _ => Err(Error::MismatchedPropertyTypeError {
                    property_name: property.name().to_string(),
                    property_value: format!("{:?}", value),
                }),
            })
    }
}

impl<DB> GetProperty for DB
where
    DB: GetDbPtr,
{
    fn get_number_of_levels(&self, cf: &impl db::ColumnFamilyLike) -> LevelNumber {
        let db_ptr = self.get_db_ptr();
        let cf_ptr = unsafe { cf.cf_ptr() };

        unsafe {
            cpp!([db_ptr as "rocksdb::DB*", cf_ptr as "rocksdb::ColumnFamilyHandle*"] -> libc::c_int as "int" {
                return db_ptr->NumberLevels(cf_ptr);
            }) as LevelNumber
        }
    }

    fn get_property(
        &self,
        cf: &impl db::ColumnFamilyLike,
        property: RocksProperty,
    ) -> Result<RocksPropertyValue> {
        op_metrics::instrument_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::GetProperty,
            move |_reporter| {
                let db_ptr = self.get_db_ptr();

                match get_property_value(property, db_ptr, cf) {
                    Some(val) => Ok(val),
                    None => Err(Error::PropertyNotFoundError {
                        property_name: property.name().to_string(),
                    }),
                }
            },
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::db::*;
    use crate::db::DbLike;
    use crate::ops::Compact;
    use crate::test::TempDbPath;

    use more_asserts::*;

    fn create_test_db() -> Result<(TempDbPath, Db, DbColumnFamily)> {
        let path = TempDbPath::new();
        let db = Db::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        // Write some data and compact to create some levels
        crate::test::fill_db(&db, &cf, 1_000)?;
        db.compact_all(&cf, None)?;

        Ok((path, db, cf))
    }

    #[test]
    fn db_get_stats() -> Result<()> {
        let (_path, db, cf) = create_test_db()?;

        // There should be at least one level
        assert_gt!(db.get_number_of_levels(&cf), 0);

        let _string_val = db.get_string_property(&cf, RocksProperty::AllStats)?;
        let _string_as_int_val = db.get_int_property(&cf, RocksProperty::NumFilesAtLevel(0))?;
        let _int_val = db.get_int_property(&cf, RocksProperty::NumImmutableMemTable)?;
        let _map_val = db.get_map_property(&cf, RocksProperty::ColumnFamilyStats)?;

        Ok(())
    }

    #[test]
    fn db_all_properties() -> Result<()> {
        let (_path, db, cf) = create_test_db()?;

        // Write some data so there's somethign in the database
        crate::test::fill_db(&db, &cf, 100_000)?;
        db.compact_all(&cf, None)?;
        crate::test::fill_db(&db, &cf, 1_000)?;

        // Get ALL possible properties one at a time
        for prop in db.all_properties(&cf) {
            match db.get_property(&cf, prop) {
                Ok(val) => {
                    println!("{}: {:#?}", prop.name(), val);
                }
                Err(e) => {
                    panic!("Error getting property '{}': {}", prop.name(), e);
                }
            }
        }

        Ok(())
    }
}
