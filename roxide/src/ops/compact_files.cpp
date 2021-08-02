//
// Created by kpovnych on 8/5/21.
//

#include "src/ops/compact_files.h"

CppStatus rocksdb_compact_files(rocksdb::DB* cpp_db,
                            rocksdb_compactoptions_t* options,
                            rocksdb_column_family_handle_t* cf,
                            const int output_level,
                            const char** file_names,
                            size_t fn_len
                        ) {

    std::vector<std::string> vec_file_names;

    auto cpp_options = cast_to_compact_options(options);

     for (size_t i = 0; i < fn_len; i++) {
        vec_file_names.push_back((file_names[i]));
    }

    auto status = cpp_db->CompactFiles(*cpp_options, cast_to_cf(cf), vec_file_names, output_level);

    return rustify_status(status);

}