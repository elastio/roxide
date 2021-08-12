/** This source file is included into the Rust code using the `cpp!` macro.
 *
 * Is in support of the `checkpoint.rs` module
 */

#include "src/checkpoint.h"

CppStatus create_checkpoint(
		rocksdb_checkpoint_t* checkpoint,
		const char * path,
		uint64_t log_size_for_flush,
		uint64_t* sequence_number
) {
    auto cpp_checkpoint = cast_to_checkpoint(checkpoint);

    auto status = cpp_checkpoint->CreateCheckpoint(std::string(path),
				log_size_for_flush,
				sequence_number);

    return rustify_status(status);
}

