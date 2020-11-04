/** This source file is included into the Rust code using the `cpp!` macro.
 *
 * Is in support of the `key_exists.rs` module
 */

#include "src/ops/key_exists.h"

CppStatus key_exists_db(rocksdb::DB* cpp_db,
	rocksdb_readoptions_t* options,
	rocksdb_column_family_handle_t* cf,
	const char* key_ptr,
	size_t key_len) {
    auto cpp_options = cast_to_read_options(options);
    auto cpp_cf = cast_to_cf(cf);

    //TODO: `PinnableSlice` will avoid a memcpy if the value is already in the block cache.
    //[This blog post](https://rocksdb.org/blog/2017/08/24/pinnableslice.html) implies it's faster that way, but it also mentions
    //some overhead associated with the pinning.  Should benchmark this compared to the non-pinnable version to see which
    //is faster.
    rocksdb::PinnableSlice dont_care;
    auto status = cpp_db->Get(*cpp_options,
	    cpp_cf,
	    rocksdb::Slice(key_ptr, key_len),
	    &dont_care
	    );

    return rustify_status(status);
}

CppStatus key_exists_tx(rocksdb_transaction_t* tx,
	rocksdb_readoptions_t* options,
	rocksdb_column_family_handle_t* cf,
	const char* key_ptr,
	size_t key_len) {
    auto cpp_tx = cast_to_tx(tx);
    auto cpp_options = cast_to_read_options(options);
    auto cpp_cf = cast_to_cf(cf);

    rocksdb::PinnableSlice dont_care;
    auto status = cpp_tx->Get(*cpp_options,
	    cpp_cf,
	    rocksdb::Slice(key_ptr, key_len),
	    &dont_care
	    );

    return rustify_status(status);
}
