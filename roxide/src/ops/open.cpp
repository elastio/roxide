/** This source file is included into the Rust code using the `cpp!` macro.
 *
 * Is in support of the `open.rs` module
 */

#include "src/ops/open.h"

std::vector<ColumnFamilyDescriptor> to_column_family_descriptors(
    int num_column_families,
    const char* const* column_family_names,
    const rocksdb_options_t* const* column_family_options)
{
    std::vector<ColumnFamilyDescriptor> column_families;
    for (int i = 0; i < num_column_families; i++) {
        column_families.emplace_back(std::string(column_family_names[i]), ColumnFamilyOptions(get_options_rep(column_family_options[i])));
    }

    return column_families;
}

template <typename TDb, typename TDbExport>
CppStatus handle_result(
    Status rocks_status,
    const std::vector<ColumnFamilyHandle*>& handles,
    rocksdb_column_family_handle_t** column_family_handles,
    TDb* db,
    TDbExport** result)
{
    auto rust_status = rustify_status(rocks_status);
    if (!rocks_status.ok()) {
        return rust_status;
    }

    for (size_t i = 0; i < handles.size(); i++) {
        column_family_handles[i] = wrap_cf(handles[i]);
    }

    *result = wrap_db(db);

    return rust_status;
}

CppStatus roxide_rocksdb_open_column_families(
    const rocksdb_options_t* options,
    const char* name,
    int num_column_families,
    const char* const* column_family_names,
    const rocksdb_options_t* const* column_family_options,
    rocksdb_column_family_handle_t** column_family_handles,
    rocksdb_t** result)
{
    auto column_families = to_column_family_descriptors(num_column_families, column_family_names, column_family_options);

    DB* db;
    std::vector<ColumnFamilyHandle*> handles;
    auto db_options = DBOptions(get_options_rep(options));
    auto rocks_status = DB::Open(db_options, std::string(name), column_families, &handles, &db);

    return handle_result(rocks_status, handles, column_family_handles, db, result);
}

CppStatus roxide_rocksdb_transactiondb_open_column_families(
    const rocksdb_options_t* options,
    const rocksdb_transactiondb_options_t* txn_db_options,
    const char* name,
    int num_column_families,
    const char* const* column_family_names,
    const rocksdb_options_t* const* column_family_options,
    rocksdb_column_family_handle_t** column_family_handles,
    rocksdb_transactiondb_t** result)
{
    auto column_families = to_column_family_descriptors(num_column_families, column_family_names, column_family_options);

    TransactionDB* txn_db;
    std::vector<ColumnFamilyHandle*> handles;
    auto rocks_status = TransactionDB::Open(get_options_rep(options),
                                            get_options_rep(txn_db_options),
                                            std::string(name),
                                            column_families,
                                            &handles,
                                            &txn_db);

    return handle_result(rocks_status, handles, column_family_handles, txn_db, result);
}

CppStatus roxide_rocksdb_open_for_read_only_column_families(
    const rocksdb_options_t* options,
    const char* name,
    int num_column_families,
    const char* const* column_family_names,
    const rocksdb_options_t* const* column_family_options,
    rocksdb_column_family_handle_t** column_family_handles,
    unsigned char error_if_wal_file_exists,
    rocksdb_t** result)
{
    auto column_families = to_column_family_descriptors(num_column_families, column_family_names, column_family_options);

    DB* db;
    std::vector<ColumnFamilyHandle*> handles;
    auto db_options = DBOptions(get_options_rep(options));
    auto rocks_status = DB::OpenForReadOnly(db_options, std::string(name), column_families, &handles, &db, error_if_wal_file_exists);

    return handle_result(rocks_status, handles, column_family_handles, db, result);
}

CppStatus roxide_rocksdb_optimistictransactiondb_open_column_families(
    const rocksdb_options_t* options,
    const char* name,
    int num_column_families,
    const char* const* column_family_names,
    const rocksdb_options_t* const* column_family_options,
    rocksdb_column_family_handle_t** column_family_handles,
    rocksdb_optimistictransactiondb_t** result)
{
    auto column_families = to_column_family_descriptors(num_column_families, column_family_names, column_family_options);

    OptimisticTransactionDB* otxn_db;
    std::vector<ColumnFamilyHandle*> handles;
    auto db_options = DBOptions(get_options_rep(options));
    auto rocks_status = OptimisticTransactionDB::Open(db_options, std::string(name), column_families, &handles, &otxn_db);

    return handle_result(rocks_status, handles, column_family_handles, otxn_db, result);
}
