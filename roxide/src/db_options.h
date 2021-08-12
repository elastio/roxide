/** This source file is included into the Rust code using the `cpp!` macro.
 *
 * Is in support of the `db_options.rs` module
 */
#pragma once

#include <unordered_map>

#include <util/stderr_logger.h>

#include "src/lib.h"
#include "src/ffi_util.h"

#include <port/port.h>
#include <port/stack_trace.h>
#include <cache/lru_cache.h>
#include <table/block_based/block_based_table_factory.h>

typedef UnorderedStringMap OptionsMap;

extern "C" {
    void set_cf_path(rocksdb_options_t* options, rocksdb_dbpath_t* db_path);
}

// Fun hack to break into the BlockBasedTableFactory and get its options
class HackBBTF : public ROCKSDB_NAMESPACE::BlockBasedTableFactory {
    public:
        // So much ugly hacking going on:
        //
        // We have made HackBBTF a child class of BlockBasedTableFactory so that we can bypass the access protections
        // and call GetOptionsPtr directly.  So we have to coerce this `factory` pointer into a pointer to ourselves
        // (thankfully in C++ the memory layout of a child class with no virtual methods or member fields is the same as
        // it's parent), and then call our `HackGetCache` method which abuses `GetOptionsPtr` to recover the actual
        // block cache.
        //
        // This is all highly unsafe, and is intended for use with tests that need to access the block cache pointer.
        static const rocksdb::Cache* GetCacheFromBlockBasedTableFactory(const rocksdb::BlockBasedTableFactory* factory) {
            auto self = reinterpret_cast<const HackBBTF*>(factory);

            return self->HackGetCache();
        }

    private:
        const ROCKSDB_NAMESPACE::Cache* HackGetCache() const {
            return reinterpret_cast<const ROCKSDB_NAMESPACE::Cache*>(this->GetOptionsPtr(ROCKSDB_NAMESPACE::TableFactory::kBlockCacheOpts()));
        }
};
