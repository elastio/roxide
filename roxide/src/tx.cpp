/** This source file is included into the Rust code using the `cpp!` macro.
 *
 * Is in support of the `lib.rs` module
 */

#include "src/tx.h"

CppStatus tx_commit(rocksdb_transaction_t* tx) {
    auto cpp_tx = cast_to_tx(tx);
    auto status = cpp_tx->Commit();
    return rustify_status(status);
}

CppStatus tx_rollback(rocksdb_transaction_t* tx) {
    auto cpp_tx = cast_to_tx(tx);
    auto status = cpp_tx->Rollback();
    return rustify_status(status);
}


