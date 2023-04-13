//! Module for enabling io_uring support in RocksDB.
//!
//! Assumes the `io_uring` feature is enabled.
//!
//! RocksDB uses an extern C function with weak linking to conditionally enable io_uring support at
//! runtime.  In our case, if that feature is enabled, then io_uring is unconditionally enabled
//! also.
use libc::c_int;

#[no_mangle]
pub extern "C" fn RocksDbIOUringEnable() -> c_int {
    1
}
