rocks_class!(FakeDbHandle, ffi::rocksdb_t, ffi::rocksdb_close, @send, @sync);
rocks_db_impl!(FakeDb, FakeDbColumnFamily, FakeDbHandle, ffi::rocksdb_t);