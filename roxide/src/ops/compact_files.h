/** This source file is included into the Rust code using the `cpp!` macro.
 *
 * Is in support of the `compact_files.rs` module
 */
#pragma once

#include "src/lib.h"
#include "src/status.h"

extern "C" {
    CppStatus rocksdb_compact_files(rocksdb::DB* cpp_db,
                            rocksdb_compactoptions_t* options,
                            rocksdb_column_family_handle_t* cf,
                            const int output_level,
                            const char** file_names,
                            size_t fn_len
                            );
}