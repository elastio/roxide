/** This source file is included into the Rust code using the `cpp!` macro.
 *
 * Is in support of the `get.rs` module
 */
#pragma once

#include "src/lib.h"
#include "src/status.h"

extern "C" {
    /** Wraps the DB::MultiGet method in a form that's a bit more convenient to call from Rust */
    void db_or_tx_multi_get(rocksdb::DB* cpp_db,
	  rocksdb_transaction_t* cpp_tx,
	  rocksdb_readoptions_t* options,
	  rocksdb_column_family_handle_t* cf,
	  const size_t num_keys,
	  const std::vector<rocksdb::Slice>& keys,
	  std::vector<rocksdb::PinnableSlice>& values,
	  std::vector<CppStatus>& statuses);

    /** Wraps the Transaction::GetForUpdate method in a form that's a bit more convenient to call from Rust */
    CppStatus tx_get_for_update(rocksdb_transaction_t* tx,
	    rocksdb_readoptions_t* options,
	    rocksdb_column_family_handle_t* cf,
	    const char* key_ptr,
	    size_t key_len,
	    char** value_ptr_ptr,
	    size_t* value_len_ptr);
}

