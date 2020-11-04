/** This source file is included into the Rust code using the `cpp!` macro.
 *
 * Is in support of the `perf.rs` module
 */
#pragma once

#include <unordered_map>
#include <sstream>

#include "src/lib.h"

typedef std::unordered_map<std::string, uint64_t> StatsMap;
