/** This source file is included into the Rust code using the `cpp!` macro.
 *
 * Is in support of the `checkpoint.rs` module
 */
#pragma once

#include "src/lib.h"
#include "src/status.h"

extern "C" {
    CppStatus create_checkpoint(
        rocksdb_checkpoint_t* checkpoint,
        const char * path,
        uint64_t log_size_for_flush,
        uint64_t* sequence_number
    );
}

