/** This source file is included into the Rust code using the `cpp!` macro.
 *
 * Is in support of the `open.rs` module
 */
#pragma once

#include "src/lib.h"
#include "src/status.h"

// Prefixed with `roxide` to avoid conflicts with corresponding functions in the RocksDB C API.
// Functionality is the same as in original functions in the RocksDB C API, but provide detailed errors instead
// of a single string.
extern "C" {
    CppStatus roxide_rocksdb_open_column_families(
        const rocksdb_options_t* options,
        const char* name,
        int num_column_families,
        const char* const* column_family_names,
        const rocksdb_options_t* const* column_family_options,
        rocksdb_column_family_handle_t** column_family_handles,
        rocksdb_t** result);

    CppStatus roxide_rocksdb_transactiondb_open_column_families(
        const rocksdb_options_t* options,
        const rocksdb_transactiondb_options_t* txn_db_options,
        const char* name,
        int num_column_families,
        const char* const* column_family_names,
        const rocksdb_options_t* const* column_family_options,
        rocksdb_column_family_handle_t** column_family_handles,
        rocksdb_transactiondb_t** result);

    CppStatus roxide_rocksdb_open_for_read_only_column_families(
        const rocksdb_options_t* options,
        const char* name,
        int num_column_families,
        const char* const* column_family_names,
        const rocksdb_options_t* const* column_family_options,
        rocksdb_column_family_handle_t** column_family_handles,
        unsigned char error_if_wal_file_exists,
        rocksdb_t** result);

    CppStatus roxide_rocksdb_optimistictransactiondb_open_column_families(
        const rocksdb_options_t* options,
        const char* name,
        int num_column_families,
        const char* const* column_family_names,
        const rocksdb_options_t* const* column_family_options,
        rocksdb_column_family_handle_t** column_family_handles,
        rocksdb_optimistictransactiondb_t** result);
}
