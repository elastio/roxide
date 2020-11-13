/** This source file is included into the Rust code using the `cpp!` macro.
 *
 * Is in support of the `flush.rs` module
 */

#include "src/ops/flush.h"

CppStatus flush_db(rocksdb::DB* cpp_db,
    rocksdb_flushoptions_t* options,
    rocksdb_column_family_handle_t** cfs,
    size_t cf_len) {
    std::vector<ColumnFamilyHandle*> cf_handles;

    auto cpp_options = cast_to_flush_options(options);

    for (size_t i = 0; i < cf_len; i++) {
        cf_handles.push_back(cast_to_cf(cfs[i]));
    }

    auto status = cpp_db->Flush(*cpp_options,
        cf_handles
    );

    return rustify_status(status);
}
