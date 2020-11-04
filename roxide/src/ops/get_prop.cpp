/** This source file is included into the Rust code using the `cpp!` macro.
 *
 * Is in support of the `get_prop.rs` module
 */

#include "src/ops/get_prop.h"

/** Gets a named string property from the database column family */
static bool get_string_property(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf, rocksdb::Slice& property_name, std::string& value) {
    return db->GetProperty(cf, property_name, &value);
}

/** Gets a named integer property from the database column family */
static bool get_int_property(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf, rocksdb::Slice& property_name, uint64_t& value) {
    return db->GetIntProperty(cf, property_name, &value);
}

/** Gets a named map property from the database column family */
static bool get_map_property(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf, rocksdb::Slice& property_name, MapValue& value) {
    return db->GetMapProperty(cf, property_name, &value);
}
