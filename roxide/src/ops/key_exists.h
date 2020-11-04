/** This source file is included into the Rust code using the `cpp!` macro.
 *
 * Is in support of the `key_exists.rs` module
 */
#pragma once

#include "src/lib.h"
#include "src/status.h"

extern "C" {
    CppStatus key_exists_db(rocksdb::DB* db,
	    rocksdb_readoptions_t* options,
	    rocksdb_column_family_handle_t* cf,
	    const char* key_ptr,
	    size_t key_len);
    CppStatus key_exists_tx(rocksdb_transaction_t * tx,
	    rocksdb_readoptions_t* options,
	    rocksdb_column_family_handle_t* cf,
	    const char* key_ptr,
	    size_t key_len);
}

