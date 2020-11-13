/** This source file is included into the Rust code using the `cpp!` macro.
 *
 * Is in support of the `lib.rs` module
 */
#pragma once

// All of the other C++ header files in this crate will include this one, as it's kind of like the project's
// main heade file.  This is also where helper functions will be declared that are shared between modules

#include <rocksdb/c.h>
#include <rocksdb/convenience.h>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/iostats_context.h>
#include <rocksdb/listener.h>
#include <rocksdb/options.h>
#include <rocksdb/perf_context.h>
#include <rocksdb/perf_level.h>
#include <rocksdb/slice.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/optimistic_transaction_db.h>
#include <rocksdb/utilities/transaction_db.h>

rocksdb::DB* cast_to_db(::rocksdb_t* db);
rocksdb::DB* cast_to_db(::rocksdb_transactiondb_t* db);
rocksdb::DB* cast_to_db(::rocksdb_optimistictransactiondb_t* db);
rocksdb::ColumnFamilyHandle* cast_to_cf(::rocksdb_column_family_handle_t* cf);
rocksdb::Transaction* cast_to_tx(::rocksdb_transaction_t* tx);
rocksdb::DbPath* cast_to_db_path(::rocksdb_dbpath_t* db_path);
rocksdb::Options* cast_to_options(::rocksdb_options_t* options);
rocksdb::ReadOptions* cast_to_read_options(::rocksdb_readoptions_t* options);
rocksdb::WriteOptions* cast_to_write_options(::rocksdb_writeoptions_t* options);
rocksdb::FlushOptions* cast_to_flush_options(::rocksdb_flushoptions_t* options);

rocksdb::Slice string_as_slice(const char* string, size_t len);

