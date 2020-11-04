/** This source file is included into the Rust code using the `cpp!` macro.
 *
 * Is in support of the `delete.rs` module
 */

#include "src/ops/delete.h"

CppStatus delete_range_db(rocksdb::DB* cpp_db,
	    rocksdb_writeoptions_t* options,
	    rocksdb_column_family_handle_t* cf,
	    const char* start_key_ptr,
	    size_t start_key_len,
	    const char* end_key_ptr,
	    size_t end_key_len) {
    auto cpp_options = cast_to_write_options(options);
    auto cpp_cf = cast_to_cf(cf);

    auto status = cpp_db->DeleteRange(*cpp_options,
	    cpp_cf,
	    rocksdb::Slice(start_key_ptr, start_key_len),
	    rocksdb::Slice(end_key_ptr, end_key_len));

    return rustify_status(status);
}

