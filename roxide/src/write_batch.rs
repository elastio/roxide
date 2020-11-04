use crate::error::prelude::*;
use crate::ffi;
use crate::handle;
use crate::rocks_class;

use std::ptr;

rocks_class!(WriteBatchHandle, ffi::rocksdb_writebatch_t, ffi::rocksdb_writebatch_destroy, @send);
/// In RocksDB terminology, a write batch is a collection of write operations which are constructed
/// and either applied atomically as one unit, or not applied at all.  It's like a lighter weight
/// version of a transaction.
///
/// Even outside of transactions it's very convenient to be able to batch a large number of writes
/// and be guaranteed that they either all succeed or all fail.
#[derive(Clone)]
pub struct WriteBatch {
    inner: WriteBatchHandle,
}

impl WriteBatch {
    pub fn new() -> Result<WriteBatch> {
        let batch = unsafe { ffi::rocksdb_writebatch_create() };

        if let Some(batch) = ptr::NonNull::new(batch) {
            Ok(WriteBatch {
                inner: WriteBatchHandle::new(batch),
            })
        } else {
            Err(Error::other_error("Failed to create new write batch"))
        }
    }

    pub fn len(&self) -> usize {
        unsafe { ffi::rocksdb_writebatch_count(self.inner.as_ptr()) as usize }
    }

    /// Return WriteBatch serialized size (in bytes).
    pub fn size_in_bytes(&self) -> usize {
        unsafe {
            let mut batch_size: libc::size_t = 0;
            ffi::rocksdb_writebatch_data(self.inner.as_ptr(), &mut batch_size);
            batch_size as usize
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear all updates buffered in this batch.
    pub fn clear(&mut self) -> Result<()> {
        unsafe {
            ffi::rocksdb_writebatch_clear(self.inner.as_ptr());
        }
        Ok(())
    }
}

impl crate::ops::RocksOpBase for WriteBatch {
    type HandleType = WriteBatchHandle;

    fn handle(&self) -> &Self::HandleType {
        &self.inner
    }
}

impl handle::RocksObject<ffi::rocksdb_writebatch_t> for WriteBatch {
    fn rocks_ptr(&self) -> ptr::NonNull<ffi::rocksdb_writebatch_t> {
        *self.inner
    }
}
