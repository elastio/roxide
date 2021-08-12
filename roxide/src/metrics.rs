//! This module contains the definitions and helper methods for all of the metrics which we
//! reporte related to a RocksDB database
//!
//! This is purely internal and is not meant to acessed outside of this crate.

use crate::labels::*;
use crate::ops::{RocksProperty, RocksPropertyValue};
use crate::Result;
use cheburashka::metrics::*;
use lazy_static::lazy_static;
use maplit::hashmap;
use snafu::ResultExt;
use std::borrow::Cow;
use std::collections::HashMap;

// Define some type aliases to make the type names of the metrics we're using a bit simpler and
// more explicit.
pub type DatabaseIntGauge = LabeledIntGauge<DatabaseLabelsStaticLabels>;
pub type DatabaseIntCounter = LabeledIntCounter<DatabaseLabelsStaticLabels>;
pub type DatabaseHistogram = LabeledHistogram<DatabaseLabelsStaticLabels>;

pub type DatabaseOperationIntGauge = LabeledIntGauge<DatabaseOperationLabelsStaticLabels>;
pub type DatabaseOperationIntCounter = LabeledIntCounter<DatabaseOperationLabelsStaticLabels>;
pub type DatabaseOperationHistogram = LabeledHistogram<DatabaseOperationLabelsStaticLabels>;

pub type ColumnFamilyFloatGauge = LabeledFloatGauge<ColumnFamilyLabelsStaticLabels>;

pub type ColumnFamilyOperationIntGauge = LabeledIntGauge<ColumnFamilyOperationLabelsStaticLabels>;
pub type ColumnFamilyOperationIntCounter =
    LabeledIntCounter<ColumnFamilyOperationLabelsStaticLabels>;
pub type ColumnFamilyOperationHistogram = LabeledHistogram<ColumnFamilyOperationLabelsStaticLabels>;

const MIN_LEVEL: usize = 0;
const MAX_LEVEL: usize = 6;

lazy_static! {
    /// The names of the RocksDB DB-level tickers (which are like Prometheus "counter" types) which should
    /// be captured and reported via the metrics system.
    ///
    /// The first string is the name of the value in the name of the RocksDB statistic that the metric
    /// comes from.  This value will also be used to make the Prometheus metric name, by replacing
    /// "." with "_".  The second string is the Prometheus metric description
    static ref ROCKS_DB_TICKER_METRICS: Vec<(&'static str, &'static str)> = vec![
        // Cache related metrics
        ("rocksdb.block.cache.add", "Number of entries added to the RocksDB block cache"),
        ("rocksdb.block.cache.add.failures", "Number of failures adding to the RocksDB block cache"),
        ("rocksdb.block.cache.hit", "Number of block cache hits"),
        ("rocksdb.block.cache.miss", "Number of block cache misses"),
        ("rocksdb.block.cache.bytes.read", "Number of bytes read from the RocksDB block cache"),
        ("rocksdb.block.cache.bytes.write", "Number of bytes written to the RocksDB block cache"),
        ("rocksdb.block.cache.data.add", "Data blocks added to the RocksDB block cache"),
        ("rocksdb.block.cache.data.bytes.insert", "Data bytes added to the RocksDB block cache"),
        ("rocksdb.block.cache.data.hit", "Hits on data blocks in RocksDB block cache"),
        ("rocksdb.block.cache.data.miss", "Misses on data blocks in RocksDB block cache"),
        ("rocksdb.block.cache.filter.add", "Filter blocks added to the RocksDB block cache"),
        ("rocksdb.block.cache.filter.bytes.insert", "Filter bytes added to the RocksDB block cache"),
        ("rocksdb.block.cache.filter.bytes.evict", "Bytes evicted from the RocksDB block cache as a result of adding filter blocks"),
        ("rocksdb.block.cache.filter.hit", "Hits on filter blocks in RocksDB block cache"),
        ("rocksdb.block.cache.filter.miss", "Misses on filter blocks in RocksDB block cache"),
        ("rocksdb.block.cache.index.add", "Index blocks added to the RocksDB block cache"),
        ("rocksdb.block.cache.index.bytes.insert", "Index bytes added to the RocksDB block cache"),
        ("rocksdb.block.cache.index.bytes.evict", "Bytes evicted from the RocksDB block cache as a result of adding index blocks"),
        ("rocksdb.block.cache.index.hit", "Hits on index blocks in RocksDB block cache"),
        ("rocksdb.block.cache.index.miss", "Misses on index blocks in RocksDB block cache"),

        // Bloom filter metrics
        // https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter
        ("rocksdb.bloom.filter.full.positive", "Number of positive key matches in a full (as opposed to prefix) bloom filter"),
        ("rocksdb.bloom.filter.full.true.positive", "Number of true (meaning not false) positive key matches in a full (as opposed to prefix) bloom filter"),
        ("rocksdb.bloom.filter.useful", "Number of times the bloom filter reported a negative and thus saved an unnecessary I/O operation"),
        ("rocksdb.bloom.filter.prefix.checked", "Number of times the prefix bloom filter was checked (includes both positive and negative results)"),
        ("rocksdb.bloom.filter.prefix.useful", "Number of times the prefix bloom filter reported a negative and thus saved an unnecessary I/O operation"),

        // I/O metrics
        ("rocksdb.bytes.read", "Total bytes read"),
        ("rocksdb.bytes.written", "Total bytes written"),
        ("rocksdb.read.amp.estimate.useful.bytes", "Estimated useful bytes read"),
        ("rocksdb.read.amp.total.read.bytes", "Total bytes read including unnecessary bytes caused by read ampliciation"),
        ("rocksdb.l0.hit", "Number of get operations served by L0"),
        ("rocksdb.l1.hit", "Number of get operations served by L1"),
        ("rocksdb.l2andup.hit", "Number of get operations served by L2 and up"),
        ("rocksdb.memtable.hit", "Number of memtable hits"),
        ("rocksdb.memtable.miss", "Number of memtable misses"),
        ("rocksdb.write.other", "Writes done for others"),
        ("rocksdb.write.self", "Writes done for self"),

        // Block metrics
        ("rocksdb.number.block.compressed", "Number of block compressions"),
        ("rocksdb.number.block.decompressed", "Number of block decompressions"),
        ("rocksdb.number.block.not_compressed", "Number of blocks not compressed"),

        // Key metrics
        ("rocksdb.number.keys.read", "Number of keys read"),
        ("rocksdb.number.keys.updated", "Number of keys updated"),
        ("rocksdb.row.cache.hit", "Row cache hits"),
        ("rocksdb.row.cache.miss", "Row cache misses"),

        // WAL metrics
        ("rocksdb.wal.bytes", "WAL bytes written"),
        ("rocksdb.wal.synced", "Number of times WAL contents are sync'd to disk"),

        // Compaction metrics
        ("rocksdb.compact.read.bytes", "Bytes read during compact operations"),
        ("rocksdb.compact.write.bytes", "Bytes written during compact operations"),
        ("rocksdb.compaction.cancelled", "Cancelled compaction operations"),
        ("rocksdb.compaction.key.drop.new", "Keys dropped during compaction because a newer value existed"),
        ("rocksdb.compaction.key.drop.obsolete", "Keys dropped during compaction because the key was obsolete"),
        ("rocksdb.compaction.key.drop.range_del", "Keys dropped during compaction because the key was in a deleted range"),
        ("rocksdb.compaction.optimized.del.drop.obsolete", "Keys deleted before the bottom level due to file gap optimization"),
        ("rocksdb.compaction.range_del.drop.obsolete", "Obsolete key range tombstones deleted"),
    ];

    /// Map of RocksDB `TickerID` ids to database-level gauges that will track the value of each
    /// ticker.
    ///
    /// Maps the rocksdb TickerID of counters we're interested in, to a `GaugeVec` that will
    /// record that ticker.  Note that not all tickers are of interest to us, so this is a subset
    /// of all available tickers.
    ///
    /// Technically, a RocksDB "ticker" is like a prometheus "Counter", in that they are
    /// monotonically increasing values.  However there's no way to set the value of a counter,
    /// only to increment it, which would require we first get the counter value (incurring a lock
    /// and memory access), compute the difference with the new value, then increment by the
    /// difference.  That's overhead that's not worth paying.
    ///
    /// Also note that currently RocksDB histograms are completely ignored, as they are not
    /// compatible with Prometheus histogram types.
    static ref ROCKS_DB_GAUGES: HashMap<crate::stats::TickerId, DatabaseIntGauge> = {
        let mut gauges = HashMap::new();
        let tickers = crate::stats::tickers_map();

        // The tickers map maps ticker IDs to the ticker name.  We need the reverse; we have the
        // name and need the ID
        let tickers: HashMap::<String, crate::stats::TickerId> = tickers.clone().drain().map(|(key, value)| (value, key)).collect();

        for (ticker_name, description) in ROCKS_DB_TICKER_METRICS.iter() {
            let ticker_id = tickers.get(*ticker_name)
                .unwrap_or_else(|| panic!("RocksDB ticker '{}' was not found in the ticker map; this is a bug or a breaking change in RocksDB", ticker_name));

            // Translate the rocks naming convention, using "." as the separator, to the prometheus
            // convention which uses "_"
            let metric_name = ticker_name.replace(".", "_");

            let gauge = DatabaseLabels::register_int_gauge(metric_name,
                *description).unwrap();

            gauges.insert(*ticker_id, gauge);
        }

        gauges
    };

    /// List of the RocksDB column family-level stats that should be reported to our metrics
    /// system.
    ///
    /// The first string is the name of the value in the `rocks.cfstats` property that the metric
    /// comes from.  This value will also be used to make the Prometheus metric name, by replacing
    /// "." with "_".  The second string is the Prometheus metric description
    static ref ROCKS_CF_STATS_METRICS: Vec<(String, String)> = {
        // There are a ton of stats in `rocks.cfstats` and they're hard to interpret.
        // This page helps: https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide

        // A few of the stats are not level-specific
        let cf_metrics = vec![
            ("io_stalls.memtable_compaction", "Number of times IO has stopped due to the memtable limit (Stall because all memtables were full, flush process couldn't keep up)"),
            ("io_stalls.memtable_slowdown", "Number of times IO has been stalled due to approaching the memtable limit in max_write_buffer_number"),
            ("io_stalls.slowdown_for_pending_compaction_bytes", "Number of times IO has been stalled due to pending compaction"),
            ("io_stalls.stop_for_pending_compaction_bytes", "Number of times IO has stopped due to pending compaction"),
            ("io_stalls.total_slowdown", "Total number of times IO has been stalled for any reason"),
            ("io_stalls.total_stop", "Total number of times IO has stopped for any reason"),
            ("io_stalls.level0_slowdown", "Number of times IO has been stalled because of level0_slowdown_writes_trigger"),
            ("io_stalls.level0_numfiles", "Number of times IO has stopped because of level0_stop_writes_trigger"),
            ("io_stalls.level0_numfiles_with_compaction", "Number of times IO has stopped because the level 0 file count limit was reached"),
            ("io_stalls.level0_slowdown_with_compaction", "Number of times IO has been stalled because of approaching the level 0 file count limit"),
        ];

        let level_metrics = vec![
            ("compaction.{N}.AvgSec", "Average time per compaction between levels"),
            ("compaction.{N}.CompCount", "Total number of compactions between levels"),
            ("compaction.{N}.CompSec", "Total time spent doing compactions between levels"),
            ("compaction.{N}.CompactedFiles", "The second is the number of files currently doing compaction"),
            ("compaction.{N}.KeyDrop", "Number of records dropped (not written out) during compaction"),
            ("compaction.{N}.KeyIn", "Number of records compared during compaction"),
            ("compaction.{N}.MovedGB", "Gigabytes moved to level {N+1} during compaction. In this case there is no IO other than updating the manifest to indicate that a file which used to be in level X is now in level Y"),
            ("compaction.{N}.NumFiles", "The number of files in the {Level}"),
            ("compaction.{N}.ReadGB", "Total gigabytes read during compaction between levels N and {N+1}. This includes bytes read from level {N} and from level {N+1}"),
            ("compaction.{N}.ReadMBps", "The rate (MB/s) at which data is read during compaction between levels N and {N+1}. This is (Read(GB) * 1024) / duration where duration is the time for which compactions are in progress from level {N} to {N+1}."),
            ("compaction.{N}.RnGB", "Gigabytes read from level {N} during compaction between levels {N} and {N+1}"),
            ("compaction.{N}.Rnp1GB", "Gigabytes read from level {N+1} during compaction between levels {N} and {N+1}"),
            ("compaction.{N}.Score", "For levels other than L0 the score is (current level size) / (max level size). Values of 0 or 1 are okay, but any value greater than 1 means that level needs to be compacted. For L0 the score is computed from the current number of files and number of files that triggers a compaction."),
            ("compaction.{N}.SizeBytes", "Size in bytes of {Level}"),
            ("compaction.{N}.WnewGB", "(Wnp1(cnt) - Rnp1(cnt)) -- Increase in file count as result of compaction between levels N and {N+1}"),
            ("compaction.{N}.WriteAmp", "(total bytes written to level {N+1}) / (total bytes read from level {N}). This is the write amplification from compaction between levels {N} and {N+1}"),
            ("compaction.{N}.WriteGB", "Total gigabytes written during compaction between levels {N} and {N+1}"),
            ("compaction.{N}.WriteMBps", "The rate (MB/s) at which data is written during compaction. See ReadMBps."),
        ];

        // Build the list of all of the CF metrics by combinting the cf metrics with the per-level
        // metrics, expanding the per-level metrics out for each possible level (plus a bonus "Sum"
        // level that aggregates all the others).
        let mut metrics: Vec<_> = cf_metrics
            .into_iter()
            .map(|(name, description)| (name.to_owned(), description.to_owned()))
            .collect();

        let mut levels: Vec<_> = (MIN_LEVEL..=MAX_LEVEL)
            .map(|level| (level, format!("L{}", level)))
            .collect();
        levels.push((0, "Sum".to_string()));

        for (level_number, level_name) in levels {
            let is_sum = level_name == "Sum";

            for (name, description) in &level_metrics {
                let name = name.replace("{N}", &level_name);

                // If this is the Sum version, `{Level}` should be replace with "all levels",
                // otherwise it's "level N"
                let description = if is_sum {
                    description.replace("{Level}", "all levels")
                } else {
                    description
                        .replace("{Level}", &format!("level {}", level_number))
                        .replace("{N}", &format!("{}", level_number))
                        .replace("{N+1}", &format!("{}", level_number + 1))
                };

                metrics.push((name, description));
            }
        }

        metrics
    };

    /// List of the RocksDB column family properties that should be exposed as metrics.
    ///
    /// Some useful metrics are exposed in a single property `cfstats`; those are desribed above in
    /// `ROCKS_CF_STATS_METRICS`.  This is a list of specific CF-level properties who are by
    /// themselves useful metrics.
    static ref ROCKS_CF_PROP_METRICS: HashMap<RocksProperty, &'static str> = {
        hashmap![
            RocksProperty::NumFilesAtLevel(1) => "The number of SST files at level 1",
            RocksProperty::NumFilesAtLevel(2) => "The number of SST files at level 2",
            RocksProperty::NumFilesAtLevel(3) => "The number of SST files at level 3",
            RocksProperty::NumFilesAtLevel(4) => "The number of SST files at level 4",
            RocksProperty::NumFilesAtLevel(5) => "The number of SST files at level 5",
            RocksProperty::NumFilesAtLevel(6) => "The number of SST files at level 6",
            RocksProperty::NumFilesAtLevel(7) => "The number of SST files at level 7",
            RocksProperty::CompressionRatioAtLevel(1) => "The compression ratio at level 1",
            RocksProperty::CompressionRatioAtLevel(2) => "The compression ratio at level 2",
            RocksProperty::CompressionRatioAtLevel(3) => "The compression ratio at level 3",
            RocksProperty::CompressionRatioAtLevel(4) => "The compression ratio at level 4",
            RocksProperty::CompressionRatioAtLevel(5) => "The compression ratio at level 5",
            RocksProperty::CompressionRatioAtLevel(6) => "The compression ratio at level 6",
            RocksProperty::CompressionRatioAtLevel(7) => "The compression ratio at level 7",
            RocksProperty::NumImmutableMemTable => "The number of immutable memtables",
            RocksProperty::NumImmutableMemTableFlushed => "The number of immutable memtables that have been flushed",
            RocksProperty::CurrentSizeActiveMemTable => "The current size of the active memtable",
            RocksProperty::CurrentSizeAllMemTables => "The current size of all memtables",
            RocksProperty::NumEntriesActiveMemTable => "The number of entries in the active memtable",
            RocksProperty::NumEntriesImmutableMemTables => "The number of entries in immutable memtables",
            RocksProperty::EstimateNumberOfKeys => "The estimated number of keys",
            RocksProperty::EstimateLiveDataSize => "The estimated live data size",
            RocksProperty::BaseLevel => "The base (meaning the largest, oldest) level",
            RocksProperty::TotalSstFilesSize => "The total size of all SST files",
            RocksProperty::LiveSstFilesSize => "The total size of live SST files",
            RocksProperty::EstimatePendingCompactionBytes => "The estimated number of bytes which are pending compaction",
            RocksProperty::NumRunningCompactions => "The number of compactions currently running",
            RocksProperty::NumRunningFlushes => "The number of flushes currently running",
            RocksProperty::BlockCacheCapacity => "The capacity of the block cache",
            RocksProperty::BlockCacheUsage => "The usage of the block cache",
            RocksProperty::BlockCachePinnedUsage => "The pinned usage of the block cache",
        ]
    };

    /// Maps column family-level `rocks.cfstat` properties and individual properties to the gauges
    /// that report those properties to our metrics system.
    ///
    /// Metrics at the column family level are reported entirely differently than at the database
    /// level.  DB level metrics are reported via the `Statistics` struct.  CF level metrics are
    /// reported via the `Properties` API, in a `rocks.cfstats` property that actually contains a
    /// map of key/value pairs.  The key in that pair is also the key in this hash; the value is
    /// the gauge that should receive the value of that particular pair.
    ///
    /// In additional there are some CF level items reported as separate named properties, which
    /// also contain useful information that we want to expose as metrics.  Both types of metrics
    /// are included in `ROCKS_CF_GAUGES`.
    static ref ROCKS_CF_GAUGES: HashMap<String, ColumnFamilyFloatGauge> = {
        let mut gauges = HashMap::new();

        // First create metrics for each of the statistics on the `rocks.cfstat` map property
        for (stat_name, description) in ROCKS_CF_STATS_METRICS.iter() {
            // Translate the rocks naming convention, using "." as the separator, to the prometheus
            // convention which uses "_"
            let metric_name = format!("rocksdb_{}", stat_name.replace(".", "_"));

            let gauge = ColumnFamilyLabels::register_float_gauge(metric_name,
                description.to_owned()).unwrap();

            gauges.insert(stat_name.to_owned(), gauge);
        }

        // Next, create metrics for the various rocks properties that we also expose
        for (property, description) in ROCKS_CF_PROP_METRICS.iter() {
            // Translate the rocks naming convention, using "." as the separator, to the prometheus
            // convention which uses "_".  Also the `-` character is not looked upon very kindly
            let property_name = property.name();
            let metric_name = property_name.replace(".", "_").replace("-", "_");

            let gauge = ColumnFamilyLabels::register_float_gauge(metric_name,
                *description).unwrap();

            gauges.insert(property_name.to_string(), gauge);
        }

        gauges
    };
}

/// Given a RocksDB database, pulls relevant metrics from the RocksDB APIs and copies them into the
/// default Prometheus registry for the process.  This should be called immediately before
/// responding to a request on a `/metrics` URL by a Prometheus scraper.
pub fn update_rocks_metrics(
    db: &(impl crate::db::DbLike + crate::ops::Stats + crate::ops::GetProperty),
) -> Result<()> {
    let labels = get_db_metric_labels(db);

    // Whether or not stats are available depends on the configured stats level.  If stats are
    // disabled that's not an error
    if let Some(stats) = db.get_db_stats() {
        // There are a ton of stats, not all of them of interest to us.  Each metric we track has a
        // cost operationally, so we try to limit ourselves to the most valuable metrics.
        // `ROCKS_DB_GAUGES` contains the list of those DB-level statistics that we are most
        // interested in
        for ticker in stats.counters {
            if let Some(gauge) = ROCKS_DB_GAUGES.get(&ticker.id) {
                let result = update_rocks_db_gauge(&labels, gauge, &ticker);

                // We only care about failures here if this is a debug build.  In production an error
                // logging or reporting metrics shouldn't take down the service
                #[cfg(debug)]
                result?;

                #[cfg(not(debug))]
                drop(result);
            }
        }
    }

    // Gather metrics for each column family
    for cf_name in db.get_cf_names() {
        let cf = db.get_cf(cf_name).expect(
            "Failed to get a CF by name from a name that was just obtained from DB; this is a bug",
        );
        // For every column family, get the `rocks.cfstats` property (yes, CF stats are not accessed
        // via the `Statistics` struct but via `Properties`; it doesn't make sense to me either).
        let cf_stats = db.get_map_property(&cf, RocksProperty::ColumnFamilyStats)?;
        let labels = get_cf_metric_labels(&cf);

        for (key, value) in cf_stats {
            if let Some(gauge) = ROCKS_CF_GAUGES.get(&key) {
                let result = update_rocks_cf_gauge_str(&labels, gauge, key, value);

                // We only care about failures here if this is a debug build.  In production an error
                // logging or reporting metrics shouldn't take down the service
                #[cfg(debug)]
                result?;

                #[cfg(not(debug))]
                drop(result);
            }
        }

        // Now for every individual column family property we're interested in, get that property
        // value and push it to the corresponding gauge
        for property in db.all_properties(&cf) {
            if ROCKS_CF_PROP_METRICS.contains_key(&property) {
                let property_name = property.name();
                let gauge = ROCKS_CF_GAUGES
                    .get(property_name.as_ref())
                    .expect("BUG: guage not found for property");
                let value = db.get_property(&cf, property)?;

                let result = match value {
                    RocksPropertyValue::IntValue(stat) => {
                        update_rocks_cf_gauge_u64(&labels, gauge, stat)
                    }
                    RocksPropertyValue::DoubleValue(stat) => {
                        update_rocks_cf_gauge_f64(&labels, gauge, stat)
                    }
                    _other => panic!("BUG: all property metrics should be int or double"),
                };

                // We only care about failures here if this is a debug build.  In production an error
                // logging or reporting metrics shouldn't take down the service
                #[cfg(debug)]
                result?;

                #[cfg(not(debug))]
                drop(result);
            }
        }
    }

    Ok(())
}

/// Updates the value of a gauge which corresponds to a RocksDB ticker
fn update_rocks_db_gauge(
    labels: &impl MetricLabels<MetricLabelsType = DatabaseLabelsStaticLabels>,
    gauge: &DatabaseIntGauge,
    ticker: &crate::stats::TickerData,
) -> Result<()> {
    gauge.apply_labels(labels)?.set(ticker.count as i64);

    Ok(())
}

/// Updates the value of a gauge which corresponds to a RocksDB column family statistic
fn update_rocks_cf_gauge_str(
    labels: &impl MetricLabels<MetricLabelsType = ColumnFamilyLabelsStaticLabels>,
    gauge: &ColumnFamilyFloatGauge,
    metric_name: impl AsRef<str>,
    stat: impl AsRef<str>,
) -> Result<()> {
    // All values should be floats but they're reported out of Rocks as strings.  Parse the string
    // as a float.
    let stat: f64 = stat
        .as_ref()
        .parse()
        .with_context(|| crate::error::InvalidMetricValue {
            name: Cow::Owned(metric_name.as_ref().to_owned()),
            value: stat.as_ref().to_owned(),
        })
        .map_err(crate::error::Error::report)?;

    update_rocks_cf_gauge_f64(labels, gauge, stat)
}

fn update_rocks_cf_gauge_u64(
    labels: &impl MetricLabels<MetricLabelsType = ColumnFamilyLabelsStaticLabels>,
    gauge: &ColumnFamilyFloatGauge,
    stat: u64,
) -> Result<()> {
    update_rocks_cf_gauge_f64(labels, gauge, stat as f64)
}

fn update_rocks_cf_gauge_f64(
    labels: &impl MetricLabels<MetricLabelsType = ColumnFamilyLabelsStaticLabels>,
    gauge: &ColumnFamilyFloatGauge,
    stat: f64,
) -> Result<()> {
    gauge.apply_labels(labels)?.set(stat);

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::db::{Db, DbLike};
    use crate::db_options::DbOptions;
    use crate::ops::{Compact, DbOpen, StatsLevel};
    use crate::test::TempDbPath;
    use crate::Result;
    use maplit::hashmap;

    #[test]
    fn update_stats_from_db() -> Result<()> {
        let path = TempDbPath::new();
        let mut options = DbOptions::default();
        options.set_stats_level(StatsLevel::Minimal);
        options.add_column_family("foo");
        options.add_column_family("bar");

        let db = Db::open(&path, options)?;

        // Generate some activity so the metrics have something to report
        // The per-level stats in the CF metrics are not even reported by RocksDB unless there has
        // been some activity on that level.
        let cf = db.get_cf("default").unwrap();
        crate::test::fill_db(&db, &cf, 10_000)?;
        db.compact_all(&cf, None)?;

        db.update_metrics()?;

        cheburashka::metrics::assert_metric_exists("rocksdb_bytes_read", None);

        cheburashka::metrics::assert_metric_exists(
            "rocksdb_compaction_L0_WriteMBps",
            hashmap![
                "cf_name".to_string() => "default".to_string()
            ],
        );

        Ok(())
    }
}
