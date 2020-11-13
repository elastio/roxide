/** This source file is included into the Rust code using the `cpp!` macro.
 *
 * Is in support of the `db_options.rs` module
 */
#pragma once

#include <unordered_map>

#include <util/stderr_logger.h>

#include "src/lib.h"
#include "src/ffi_util.h"

typedef UnorderedStringMap OptionsMap;

extern "C" {
    void set_cf_path(rocksdb_options_t* options, rocksdb_dbpath_t* db_path);
}
