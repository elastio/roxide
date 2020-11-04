/** This source file is included into the Rust code using the `cpp!` macro.
 *
 * Is in support of the `status.rs` module
 */

#include "src/status.h"

#ifdef OS_WIN
#include <string.h>
#endif
#include <cstring>

CppStatus rustify_status(rocksdb::Status status) {
    const char* state = nullptr;

    if (status.getState() != nullptr) {
	const size_t cch = std::strlen(status.getState()) + 1;  // +1 for the null terminator
	state = std::strncpy(new char[cch], status.getState(), cch);
    }

    return CppStatus { status.code(), status.subcode(), status.severity(), state };
}
