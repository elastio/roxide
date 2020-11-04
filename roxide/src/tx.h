/** This source file is included into the Rust code using the `cpp!` macro.
 *
 * Is in support of the `tx.rs` module
 */
#pragma once

#include "src/lib.h"
#include "src/status.h"

extern "C" {
    /** Call Transaction::Commit() more easily from Rust */
    CppStatus tx_commit(rocksdb_transaction_t* tx);

    /** Call Transaction::Rollback() more easily from Rust */
    CppStatus tx_rollback(rocksdb_transaction_t* tx);
}
