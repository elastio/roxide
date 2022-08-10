/** This source file is included into the Rust code using the `cpp!` macro.
 *
 * Is in support of the `compact.rs` module
 */

#include "src/ops/compact.h"

CppStatus compact_range_db(rocksdb::DB* cpp_db,
    rocksdb_compactoptions_t* options,
    rocksdb_column_family_handle_t* cf,
    const char* start_key_ptr,
    size_t start_key_len,
    const char* end_key_ptr,
    size_t end_key_len) {
    auto cpp_options = cast_to_compact_options(options);
    auto cpp_cf = cast_to_cf(cf);

    auto start = rocksdb::Slice(start_key_ptr, start_key_len);
    auto end = rocksdb::Slice(end_key_ptr, end_key_len);

    auto status = cpp_db->CompactRange(*cpp_options,
	    cpp_cf,
	    &start,
	    &end);

    return rustify_status(status);
}

