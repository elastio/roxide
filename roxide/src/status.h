/** This source file is included into the Rust code using the `cpp!` macro.
 *
 * Is in support of the `status.rs` module
 */
#pragma once

#include "src/lib.h"

/** A hack of a struct which interoperates with the Rust struct of the same name and binary layout */
struct CppStatus {
    rocksdb::Status::Code code;
    rocksdb::Status::SubCode subcode;
    rocksdb::Status::Severity severity;
    const char* state;
};

typedef struct CppStatus CppStatus;

CppStatus rustify_status(rocksdb::Status status);
