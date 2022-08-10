/** This source file is included into the Rust code using the `cpp!` macro.
 *
 * Is in support of the `compact.rs` module
 */
#pragma once

#include "src/lib.h"
#include "src/status.h"

extern "C" {
    CppStatus compact_range_db(rocksdb::DB* db,
	    rocksdb_compactoptions_t* options,
	    rocksdb_column_family_handle_t* cf,
	    const char* start_key_ptr,
	    size_t start_key_len,
	    const char* end_key_ptr,
	    size_t end_key_len);
}

