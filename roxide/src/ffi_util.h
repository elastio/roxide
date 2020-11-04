/** This source file is included into the Rust code using the `cpp!` macro.
 *
 * Is in support of the `ffi_util.rs` module
 */
#pragma once

#include <unordered_map>

#include "src/lib.h"

typedef std::unordered_map<std::string, std::string> UnorderedStringMap;
