/** This source file is included into the Rust code using the `cpp!` macro.
 *
 * Is in support of the `get.rs` module
 */

#include "src/ops/get.h"


/* Copied verbatim from RocksDB code */
static char* CopyString(const std::string& str) {
  char* result = reinterpret_cast<char*>(malloc(sizeof(char) * str.size()));
  memcpy(result, str.data(), sizeof(char) * str.size());
  return result;
}

void db_or_tx_multi_get(rocksdb::DB* cpp_db,
	  rocksdb_transaction_t* cpp_tx,
	  rocksdb_readoptions_t* options,
	  rocksdb_column_family_handle_t* cf,
	  const size_t num_keys,
	  const std::vector<rocksdb::Slice>& keys,
	  std::vector<rocksdb::PinnableSlice>& values,
	  std::vector<CppStatus>& statuses) {
    // Get the C++ objects from the opaque C pointers
    auto cpp_options = cast_to_read_options(options);
    auto cpp_cf = cast_to_cf(cf);

    // Prepare the output vectors with initial empty values to be filled in by the `MultiGet` call
    auto rocks_statuses = std::vector<rocksdb::Status>(num_keys, rocksdb::Status::OK());
    values.reserve(num_keys);
    for (size_t i = 0; i < num_keys; i++) {
      values.emplace_back(rocksdb::PinnableSlice());
    }

    if (cpp_db) {
      cpp_db->MultiGet(*cpp_options,
	  cpp_cf,
	  num_keys,
	  keys.data(),
	  values.data(),
	  rocks_statuses.data(),
	  false // TODO: should we expose this `is_sorted` as a parameter?
	  );
    } else {
      cast_to_tx(cpp_tx)->MultiGet(*cpp_options,
	  cpp_cf,
	  num_keys,
	  keys.data(),
	  values.data(),
	  rocks_statuses.data(),
	  false // TODO: should we expose this `is_sorted` as a parameter?
	  );
    }

    statuses.reserve(num_keys);
    for (auto& rocks_status : rocks_statuses) {
      statuses.emplace_back(rustify_status(rocks_status));
    }
}

CppStatus tx_get_for_update(rocksdb_transaction_t* tx,
	rocksdb_readoptions_t* options,
	rocksdb_column_family_handle_t* cf,
	const char* key_ptr,
	size_t key_len,
	char** value_ptr_ptr,
	size_t* value_len_ptr) {
    // Get the C++ objects from the opaque C pointers
    auto cpp_tx = cast_to_tx(tx);
    auto cpp_options = cast_to_read_options(options);
    auto cpp_cf = cast_to_cf(cf);

    // Call the C++ method
    std::string value;

    auto status = cpp_tx->GetForUpdate(*cpp_options,
	    cpp_cf,
	    rocksdb::Slice(key_ptr, key_len),
	    &value);

    if (status.ok()) {
	*value_len_ptr = value.size();
	*value_ptr_ptr = CopyString(value);
    } else {
	*value_len_ptr = 0;
	*value_ptr_ptr = nullptr;
    }

    return rustify_status(status);
}
