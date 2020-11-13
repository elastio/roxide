/** This source file is included into the Rust code using the `cpp!` macro.
 *
 * Is in support of the `flush.rs` module
 */
#pragma once

#include "src/lib.h"
#include "src/status.h"

extern "C" {
    CppStatus flush_db(rocksdb::DB* db,
	    rocksdb_flushoptions_t* options,
	    rocksdb_column_family_handle_t** cfs,
	    size_t cf_len);
}

