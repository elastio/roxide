#include "db_options.h"

void set_cf_path(rocksdb_options_t* options, rocksdb_dbpath_t* db_path) {
    auto cpp_options = cast_to_options(options);
    auto cpp_db_path = cast_to_db_path(db_path);

    std::vector<rocksdb::DbPath> paths;
    paths.push_back(*cpp_db_path);

    cpp_options->cf_paths = paths;
}
