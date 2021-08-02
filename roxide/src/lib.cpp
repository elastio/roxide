/** This source file is included into the Rust code using the `cpp!` macro.
 *
 * Is in support of the `lib.rs` module
 */

#include "src/lib.h"

//WARNING: This is a really nasty hack.  I need to be able to see inside of the opaque `rocksdb_t`
//type structures to get the actual C++ `DB` object inside.  I've copied/pasted from `c.cc` in the
//RocksDB source code to get these structure decls.  If those ever change this will break very
//catastrophically and at runtime
namespace rocksdb_hack {
    extern "C" {
        struct rocksdb_t                 { rocksdb::DB*               rep; };

        struct rocksdb_transactiondb_t {
          rocksdb::TransactionDB* rep;
        };

        struct rocksdb_optimistictransactiondb_t {
          rocksdb::OptimisticTransactionDB* rep;
        };

	struct rocksdb_column_family_handle_t  { rocksdb::ColumnFamilyHandle* rep; };

	struct rocksdb_transaction_t {
	    rocksdb::Transaction* rep;
	};

    struct rocksdb_dbpath_t {
        rocksdb::DbPath rep;
    };

	struct rocksdb_readoptions_t {
	    rocksdb::ReadOptions rep;
	   // stack variables to set pointers to in ReadOptions
	    rocksdb::Slice upper_bound;
	    rocksdb::Slice lower_bound;
	};
	struct rocksdb_writeoptions_t    { rocksdb::WriteOptions      rep; };
	struct rocksdb_options_t         { rocksdb::Options           rep; };
    struct rocksdb_flushoptions_t    { rocksdb::FlushOptions      rep; };
    struct rocksdb_compactoptions_t  { rocksdb::CompactionOptions rep; };
    }
}

static rocksdb::DB* cast_to_db_internal(struct rocksdb_hack::rocksdb_t* db) {
    return db->rep;
}

static rocksdb::DB* cast_to_db_internal(struct rocksdb_hack::rocksdb_transactiondb_t* db) {
    return db->rep;
}

static rocksdb::DB* cast_to_db_internal(struct rocksdb_hack::rocksdb_optimistictransactiondb_t* db) {
    return db->rep;
}

static rocksdb::ColumnFamilyHandle* cast_to_cf_internal(struct rocksdb_hack::rocksdb_column_family_handle_t* cf) {
    return cf->rep;
}

static rocksdb::Transaction* cast_to_tx_internal(struct rocksdb_hack::rocksdb_transaction_t* tx) {
    return tx->rep;
}

static rocksdb::DbPath* cast_to_db_path_internal(struct rocksdb_hack::rocksdb_dbpath_t* db_path) {
    return &db_path->rep;
}

static rocksdb::Options* cast_to_options_internal(struct rocksdb_hack::rocksdb_options_t* options) {
    return &options->rep;
}

static rocksdb::ReadOptions* cast_to_read_options_internal(struct rocksdb_hack::rocksdb_readoptions_t* options) {
    return &options->rep;
}

static rocksdb::WriteOptions* cast_to_write_options_internal(struct rocksdb_hack::rocksdb_writeoptions_t* options) {
    return &options->rep;
}

static rocksdb::FlushOptions* cast_to_flush_options_internal(struct rocksdb_hack::rocksdb_flushoptions_t* options) {
    return &options->rep;
}

static rocksdb::CompactionOptions* cast_to_compact_options_internal(struct rocksdb_hack::rocksdb_compactoptions_t* options) {
    return &options->rep;
}

rocksdb::DB* cast_to_db(::rocksdb_t* db) {
    return cast_to_db_internal(reinterpret_cast<struct rocksdb_hack::rocksdb_t*>(db));
}

rocksdb::DB* cast_to_db(::rocksdb_transactiondb_t* db) {
    return cast_to_db_internal(reinterpret_cast<struct rocksdb_hack::rocksdb_transactiondb_t*>(db));
}

rocksdb::DB* cast_to_db(::rocksdb_optimistictransactiondb_t* db) {
    return cast_to_db_internal(reinterpret_cast<struct rocksdb_hack::rocksdb_optimistictransactiondb_t*>(db));
}

rocksdb::ColumnFamilyHandle* cast_to_cf(::rocksdb_column_family_handle_t* cf) {
    return cast_to_cf_internal(reinterpret_cast<struct rocksdb_hack::rocksdb_column_family_handle_t*>(cf));
}

rocksdb::Transaction* cast_to_tx(::rocksdb_transaction_t* tx) {
    return cast_to_tx_internal(reinterpret_cast<struct rocksdb_hack::rocksdb_transaction_t*>(tx));
}

rocksdb::DbPath* cast_to_db_path(::rocksdb_dbpath_t* db_path) {
    return cast_to_db_path_internal(reinterpret_cast<struct rocksdb_hack::rocksdb_dbpath_t*>(db_path));
}

rocksdb::Options* cast_to_options(::rocksdb_options_t* options) {
    return cast_to_options_internal(reinterpret_cast<struct rocksdb_hack::rocksdb_options_t*>(options));
}


rocksdb::ReadOptions* cast_to_read_options(::rocksdb_readoptions_t* options) {
    return cast_to_read_options_internal(reinterpret_cast<struct rocksdb_hack::rocksdb_readoptions_t*>(options));
}


rocksdb::WriteOptions* cast_to_write_options(::rocksdb_writeoptions_t* options) {
    return cast_to_write_options_internal(reinterpret_cast<struct rocksdb_hack::rocksdb_writeoptions_t*>(options));
}

rocksdb::FlushOptions* cast_to_flush_options(::rocksdb_flushoptions_t* options) {
    return cast_to_flush_options_internal(reinterpret_cast<struct rocksdb_hack::rocksdb_flushoptions_t*>(options));
}

rocksdb::CompactionOptions* cast_to_compact_options(::rocksdb_compactoptions_t* options) {
    return cast_to_compact_options_internal(reinterpret_cast<struct rocksdb_hack::rocksdb_compactoptions_t*>(options));
}

rocksdb::Slice string_as_slice(const char* string, size_t len) {
    return rocksdb::Slice(string, len);
}

