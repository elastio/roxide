/** This CPP code is meant to be included from a `cpp!` macro in Rust code.
 *
 * It provides macros and a helper function to interact with the RocksDB performance context.
 */

#include "src/perf.h"

#define OUTPUT_PERF_COUNTER(stats, map, counter_name) (map)[#counter_name] = (stats).counter_name
#define OUTPUT_PER_LEVEL_PERF_COUNTER(stats, map, counter_name) \
    if ((stats).per_level_perf_context_enabled && (stats).level_to_perf_context) { \
        for (auto& kv: *(stats).level_to_perf_context) { \
          std::ostringstream ss; \
          ss << #counter_name << "_level" << kv.first; \
          (map)[ss.str()] = kv.second.counter_name; \
        } \
    }

// Helper function which fills a C++ map struct with values from the performance context struct
inline void perf_stats_to_map(const rocksdb::PerfContext& stats, StatsMap& map) {
  OUTPUT_PERF_COUNTER(stats, map, user_key_comparison_count);
  OUTPUT_PERF_COUNTER(stats, map, block_cache_hit_count);
  OUTPUT_PERF_COUNTER(stats, map, block_read_count);
  OUTPUT_PERF_COUNTER(stats, map, block_read_byte);
  OUTPUT_PERF_COUNTER(stats, map, block_read_time);
  OUTPUT_PERF_COUNTER(stats, map, block_cache_index_hit_count);
  OUTPUT_PERF_COUNTER(stats, map, index_block_read_count);
  OUTPUT_PERF_COUNTER(stats, map, block_cache_filter_hit_count);
  OUTPUT_PERF_COUNTER(stats, map, filter_block_read_count);
  OUTPUT_PERF_COUNTER(stats, map, compression_dict_block_read_count);
  OUTPUT_PERF_COUNTER(stats, map, block_checksum_time);
  OUTPUT_PERF_COUNTER(stats, map, block_decompress_time);
  OUTPUT_PERF_COUNTER(stats, map, get_read_bytes);
  OUTPUT_PERF_COUNTER(stats, map, multiget_read_bytes);
  OUTPUT_PERF_COUNTER(stats, map, iter_read_bytes);
  OUTPUT_PERF_COUNTER(stats, map, internal_key_skipped_count);
  OUTPUT_PERF_COUNTER(stats, map, internal_delete_skipped_count);
  OUTPUT_PERF_COUNTER(stats, map, internal_recent_skipped_count);
  OUTPUT_PERF_COUNTER(stats, map, internal_merge_count);
  OUTPUT_PERF_COUNTER(stats, map, write_wal_time);
  OUTPUT_PERF_COUNTER(stats, map, get_snapshot_time);
  OUTPUT_PERF_COUNTER(stats, map, get_from_memtable_time);
  OUTPUT_PERF_COUNTER(stats, map, get_from_memtable_count);
  OUTPUT_PERF_COUNTER(stats, map, get_post_process_time);
  OUTPUT_PERF_COUNTER(stats, map, get_from_output_files_time);
  OUTPUT_PERF_COUNTER(stats, map, seek_on_memtable_time);
  OUTPUT_PERF_COUNTER(stats, map, seek_on_memtable_count);
  OUTPUT_PERF_COUNTER(stats, map, next_on_memtable_count);
  OUTPUT_PERF_COUNTER(stats, map, prev_on_memtable_count);
  OUTPUT_PERF_COUNTER(stats, map, seek_child_seek_time);
  OUTPUT_PERF_COUNTER(stats, map, seek_child_seek_count);
  OUTPUT_PERF_COUNTER(stats, map, seek_min_heap_time);
  OUTPUT_PERF_COUNTER(stats, map, seek_internal_seek_time);
  OUTPUT_PERF_COUNTER(stats, map, find_next_user_entry_time);
  OUTPUT_PERF_COUNTER(stats, map, write_pre_and_post_process_time);
  OUTPUT_PERF_COUNTER(stats, map, write_memtable_time);
  OUTPUT_PERF_COUNTER(stats, map, write_thread_wait_nanos);
  OUTPUT_PERF_COUNTER(stats, map, write_scheduling_flushes_compactions_time);
  OUTPUT_PERF_COUNTER(stats, map, db_mutex_lock_nanos);
  OUTPUT_PERF_COUNTER(stats, map, db_condition_wait_nanos);
  OUTPUT_PERF_COUNTER(stats, map, merge_operator_time_nanos);
  OUTPUT_PERF_COUNTER(stats, map, write_delay_time);
  OUTPUT_PERF_COUNTER(stats, map, read_index_block_nanos);
  OUTPUT_PERF_COUNTER(stats, map, read_filter_block_nanos);
  OUTPUT_PERF_COUNTER(stats, map, new_table_block_iter_nanos);
  OUTPUT_PERF_COUNTER(stats, map, new_table_iterator_nanos);
  OUTPUT_PERF_COUNTER(stats, map, block_seek_nanos);
  OUTPUT_PERF_COUNTER(stats, map, find_table_nanos);
  OUTPUT_PERF_COUNTER(stats, map, bloom_memtable_hit_count);
  OUTPUT_PERF_COUNTER(stats, map, bloom_memtable_miss_count);
  OUTPUT_PERF_COUNTER(stats, map, bloom_sst_hit_count);
  OUTPUT_PERF_COUNTER(stats, map, bloom_sst_miss_count);
  OUTPUT_PERF_COUNTER(stats, map, key_lock_wait_time);
  OUTPUT_PERF_COUNTER(stats, map, key_lock_wait_count);
  OUTPUT_PERF_COUNTER(stats, map, env_new_sequential_file_nanos);
  OUTPUT_PERF_COUNTER(stats, map, env_new_random_access_file_nanos);
  OUTPUT_PERF_COUNTER(stats, map, env_new_writable_file_nanos);
  OUTPUT_PERF_COUNTER(stats, map, env_reuse_writable_file_nanos);
  OUTPUT_PERF_COUNTER(stats, map, env_new_random_rw_file_nanos);
  OUTPUT_PERF_COUNTER(stats, map, env_new_directory_nanos);
  OUTPUT_PERF_COUNTER(stats, map, env_file_exists_nanos);
  OUTPUT_PERF_COUNTER(stats, map, env_get_children_nanos);
  OUTPUT_PERF_COUNTER(stats, map, env_get_children_file_attributes_nanos);
  OUTPUT_PERF_COUNTER(stats, map, env_delete_file_nanos);
  OUTPUT_PERF_COUNTER(stats, map, env_create_dir_nanos);
  OUTPUT_PERF_COUNTER(stats, map, env_create_dir_if_missing_nanos);
  OUTPUT_PERF_COUNTER(stats, map, env_delete_dir_nanos);
  OUTPUT_PERF_COUNTER(stats, map, env_get_file_size_nanos);
  OUTPUT_PERF_COUNTER(stats, map, env_get_file_modification_time_nanos);
  OUTPUT_PERF_COUNTER(stats, map, env_rename_file_nanos);
  OUTPUT_PERF_COUNTER(stats, map, env_link_file_nanos);
  OUTPUT_PERF_COUNTER(stats, map, env_lock_file_nanos);
  OUTPUT_PERF_COUNTER(stats, map, env_unlock_file_nanos);
  OUTPUT_PERF_COUNTER(stats, map, env_new_logger_nanos);
  OUTPUT_PERF_COUNTER(stats, map, get_cpu_nanos);
  OUTPUT_PERF_COUNTER(stats, map, iter_next_cpu_nanos);
  OUTPUT_PERF_COUNTER(stats, map, iter_prev_cpu_nanos);
  OUTPUT_PERF_COUNTER(stats, map, iter_seek_cpu_nanos);

  OUTPUT_PER_LEVEL_PERF_COUNTER(stats, map, bloom_filter_useful);
  OUTPUT_PER_LEVEL_PERF_COUNTER(stats, map, bloom_filter_full_positive);
  OUTPUT_PER_LEVEL_PERF_COUNTER(stats, map, bloom_filter_full_true_positive);
  OUTPUT_PER_LEVEL_PERF_COUNTER(stats, map, block_cache_hit_count);
  OUTPUT_PER_LEVEL_PERF_COUNTER(stats, map, block_cache_miss_count);
}

inline void io_stats_to_map(const rocksdb::IOStatsContext& stats, StatsMap& map) {
  OUTPUT_PERF_COUNTER(stats, map, thread_pool_id);
  OUTPUT_PERF_COUNTER(stats, map, bytes_read);
  OUTPUT_PERF_COUNTER(stats, map, bytes_written);
  OUTPUT_PERF_COUNTER(stats, map, open_nanos);
  OUTPUT_PERF_COUNTER(stats, map, allocate_nanos);
  OUTPUT_PERF_COUNTER(stats, map, write_nanos);
  OUTPUT_PERF_COUNTER(stats, map, read_nanos);
  OUTPUT_PERF_COUNTER(stats, map, range_sync_nanos);
  OUTPUT_PERF_COUNTER(stats, map, fsync_nanos);
  OUTPUT_PERF_COUNTER(stats, map, prepare_write_nanos);
  OUTPUT_PERF_COUNTER(stats, map, logger_nanos);
}

StatsMap get_stats() {
    StatsMap map;

    perf_stats_to_map(*rocksdb::get_perf_context(), map);

    // As of version 6.3.6 of RocksDB, the iostats_context.cc code seems to be incorrect in that
    // it does not use the same `thread_local` keyword or conditional compilation logic as `perf_context.cc`.
    // These stats aren't that important to us anyway, so they're just excluded
    //io_stats_to_map(*rocksdb::get_iostats_context(), map);

    return map;
}
