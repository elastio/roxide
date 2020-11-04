//! RocksDB's C++ API allows users to implement an `RocksDbEventListener` interface and register callbacks
//! when interesting events happen.  There is a huge surface area to this and a lot of options,
//! most of which are not exposed here because we haven't had a need for them.
use crate::ffi_util::{self, DynTraitWrapper};
use std::ffi;
use std::path;

// A lot of C++ code is going to be generated in this module with the `cpp!` macro.  This top level
// invocation controls what is placed at the top of the C++ source file it generates.  So we set up
// includes and `using` statements.
//
// In C++ make an implementation of the `RocksDbEventListener` which actually translates the calls into
// the Rust implementation of the same interface.
//
// Normally large blocks of C++ code are placed in a `<modname>.cpp` file and `#include`d from the
// Rust file, but the `cpp!` macro won't be able to expand the `rust!` blocks into automatically
// generated C++ calls into Rust if we do it that way.
cpp! {{
    #include "src/lib.h"

    class RustEventListener : public rocksdb::EventListener {
    public:
        explicit RustEventListener(void* rust_boxed_listener)
            : rocksdb::EventListener(),
            rust_boxed_listener_(rust_boxed_listener)
        {
        }

        virtual ~RustEventListener() {
            // Free the boxed listener
            rust!(RustEventListener_free_boxed_listener [rust_boxed_listener_ : *mut std::ffi::c_void as "void*"] {
                unsafe {
                    CppListenerWrapper::free_raw_void(rust_boxed_listener_);
                }

            });

            rust_boxed_listener_ = nullptr;
        }

        virtual void OnFlushCompleted(rocksdb::DB* /*db*/, const rocksdb::FlushJobInfo& flush_job_info ) {
            auto job_info_ptr = &flush_job_info;
            rust!(RustEventListener_flush_completed [rust_boxed_listener_: *mut std::ffi::c_void as "void*", job_info_ptr: *const ffi::c_void as "const rocksdb::FlushJobInfo *"] {
                unsafe {
                    CppListenerWrapper::temp_from_raw_void(rust_boxed_listener_, |listener| {
                        listener.flush_completed(job_info_ptr);
                    });
                }
            });
        }

        virtual void OnCompactionCompleted(rocksdb::DB* /*db*/, const rocksdb::CompactionJobInfo& compaction_job_info) {
            auto job_info_ptr = &compaction_job_info;
            rust!(RustEventListener_compaction_completed [rust_boxed_listener_: *mut std::ffi::c_void as "void*", job_info_ptr: *const ffi::c_void as "const rocksdb::CompactionJobInfo *"] {
                unsafe {
                    CppListenerWrapper::temp_from_raw_void(rust_boxed_listener_, |listener| {
                        listener.compaction_completed(job_info_ptr);
                    });
                }
            });
        }

    private:
        void* rust_boxed_listener_;
    };
}}

/// Detailed information about a flush job.  Wraps a pointer to a RocksDB `FlushJobInfo` class.
pub struct FlushJobInfo {
    raw_ptr: *const ffi::c_void,
}

impl FlushJobInfo {
    unsafe fn new(raw: *const ffi::c_void) -> Self {
        FlushJobInfo { raw_ptr: raw }
    }

    pub fn cf_id(&self) -> u32 {
        let raw_ptr = self.raw_ptr;

        unsafe {
            cpp!([raw_ptr as "const rocksdb::FlushJobInfo*"] -> u32 as "uint32_t" {
                return raw_ptr->cf_id;
            })
        }
    }

    pub fn cf_name(&self) -> ffi::CString {
        let raw_ptr = self.raw_ptr;
        unsafe {
            let cstr_ptr = cpp!([raw_ptr as "const rocksdb::FlushJobInfo*"] -> *const libc::c_char as "const char*" {
                return raw_ptr->cf_name.c_str();
            });

            ffi::CStr::from_ptr(cstr_ptr).to_owned()
        }
    }

    pub fn file_path(&self) -> path::PathBuf {
        let raw_ptr = self.raw_ptr;
        unsafe {
            let cstr_ptr = cpp!([raw_ptr as "const rocksdb::FlushJobInfo*"] -> *const libc::c_char as "const char*" {
                return raw_ptr->file_path.c_str();
            });

            ffi_util::path_from_char_ptr(cstr_ptr)
        }
    }

    pub fn job_id(&self) -> usize {
        let raw_ptr = self.raw_ptr;

        let job_id = unsafe {
            cpp!([raw_ptr as "const rocksdb::FlushJobInfo*"] -> libc::c_int as "int" {
                return raw_ptr->job_id;
            })
        };

        job_id as usize
    }

    pub fn triggered_writes_slowdown(&self) -> bool {
        let raw_ptr = self.raw_ptr;

        let flag = unsafe {
            cpp!([raw_ptr as "const rocksdb::FlushJobInfo*"] -> bool as "bool" {
                return raw_ptr->triggered_writes_slowdown;
            })
        };

        flag as bool
    }

    pub fn triggered_writes_stop(&self) -> bool {
        let raw_ptr = self.raw_ptr;

        let flag = unsafe {
            cpp!([raw_ptr as "const rocksdb::FlushJobInfo*"] -> bool as "bool" {
                return raw_ptr->triggered_writes_stop;
            })
        };

        flag as bool
    }
}

/// Detailed information about a compaction job.  Wraps a pointer to a RocksDB `CompactionJobInfo` class.
pub struct CompactionJobInfo {
    raw_ptr: *const ffi::c_void,
}

impl CompactionJobInfo {
    unsafe fn new(raw: *const ffi::c_void) -> Self {
        CompactionJobInfo { raw_ptr: raw }
    }

    pub fn cf_id(&self) -> u32 {
        let raw_ptr = self.raw_ptr;

        unsafe {
            cpp!([raw_ptr as "const rocksdb::CompactionJobInfo*"] -> u32 as "uint32_t" {
                return raw_ptr->cf_id;
            })
        }
    }

    pub fn cf_name(&self) -> ffi::CString {
        let raw_ptr = self.raw_ptr;
        unsafe {
            let cstr_ptr = cpp!([raw_ptr as "const rocksdb::CompactionJobInfo*"] -> *const libc::c_char as "const char*" {
                return raw_ptr->cf_name.c_str();
            });

            ffi::CStr::from_ptr(cstr_ptr).to_owned()
        }
    }

    pub fn job_id(&self) -> usize {
        let raw_ptr = self.raw_ptr;

        let job_id = unsafe {
            cpp!([raw_ptr as "const rocksdb::CompactionJobInfo*"] -> libc::c_int as "int" {
                return raw_ptr->job_id;
            })
        };

        job_id as usize
    }

    pub fn base_input_level(&self) -> usize {
        let raw_ptr = self.raw_ptr;

        let base_input_level = unsafe {
            cpp!([raw_ptr as "const rocksdb::CompactionJobInfo*"] -> libc::c_int as "int" {
                return raw_ptr->base_input_level;
            })
        };

        base_input_level as usize
    }

    pub fn output_level(&self) -> usize {
        let raw_ptr = self.raw_ptr;

        let output_level = unsafe {
            cpp!([raw_ptr as "const rocksdb::CompactionJobInfo*"] -> libc::c_int as "int" {
                return raw_ptr->output_level;
            })
        };

        output_level as usize
    }

    /// The files which were the inputs to this compaction operation.  Their contents will be
    /// merged into the output files, and the input files deleted.
    pub fn input_files(&self) -> Vec<path::PathBuf> {
        let raw_ptr = self.raw_ptr;

        unsafe {
            let stl_vector_ptr = cpp!([raw_ptr as "const rocksdb::CompactionJobInfo*"] -> *const libc::c_void as "const std::vector<std::string> *" {
                return &raw_ptr->input_files;
            });

            let vec = ffi_util::stl_str_vector_to_vec(stl_vector_ptr);
            vec.into_iter()
                .map(|ptr| ffi_util::path_from_char_ptr(ptr))
                .collect()
        }
    }

    /// The files which were the inputs to this compaction operation.  Their contents will be
    /// merged into the output files, and the input files deleted.
    pub fn output_files(&self) -> Vec<path::PathBuf> {
        let raw_ptr = self.raw_ptr;

        unsafe {
            let stl_vector_ptr = cpp!([raw_ptr as "const rocksdb::CompactionJobInfo*"] -> *const libc::c_void as "const std::vector<std::string> *" {
                return &raw_ptr->output_files;
            });

            let vec = ffi_util::stl_str_vector_to_vec(stl_vector_ptr);
            vec.into_iter()
                .map(|ptr| ffi_util::path_from_char_ptr(ptr))
                .collect()
        }
    }
}

/// Receives callbacks directly from RocksDB code when interesting events happen.  Users can
/// implement this trait and override any methods which correspond to an event they're interested
/// in.
///
/// NOTE: It is vital that the handlers not block or perform any complex computation.  Anything of
/// that sort should be quickly moved off-thread with a future or some other mechanism.   It should
/// go without saying also that if any of these handler functions panic, the process will crash
/// catastrophically and the database files will be in a crash-consistent state.
pub trait RocksDbEventListener: Send + Sync {
    fn flush_completed(&self, _job_info: &FlushJobInfo) {}

    fn compaction_completed(&self, _job_info: &CompactionJobInfo) {}
}

make_dyn_trait_wrapper!(pub(crate) CppListenerWrapper, RocksDbEventListener);

impl CppListenerWrapper {
    unsafe fn flush_completed(&self, job_info_ptr: *const ffi::c_void) {
        self.0.flush_completed(&FlushJobInfo::new(job_info_ptr));
    }

    unsafe fn compaction_completed(&self, job_info_ptr: *const ffi::c_void) {
        self.0
            .compaction_completed(&CompactionJobInfo::new(job_info_ptr));
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::{db::DB, DBLike};
    use crate::db_options::DBOptions;
    use crate::ops::{Compact, DBOpen, Flush};
    use crate::test::TempDBPath;
    use crate::Result;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    struct CountingTestListener {
        job_ids: Arc<Mutex<Vec<usize>>>,
        flushes: Arc<AtomicU64>,
        compactions: Arc<AtomicU64>,
    }

    impl CountingTestListener {
        fn new() -> Self {
            CountingTestListener {
                job_ids: Arc::new(Mutex::new(Vec::new())),
                flushes: Arc::new(AtomicU64::new(0)),
                compactions: Arc::new(AtomicU64::new(0)),
            }
        }

        fn add_job_id(&self, job_id: usize) {
            let mut job_ids = self.job_ids.lock().unwrap();
            job_ids.push(job_id);
        }
    }

    impl Drop for CountingTestListener {
        fn drop(&mut self) {}
    }

    impl RocksDbEventListener for CountingTestListener {
        fn flush_completed(&self, job_info: &FlushJobInfo) {
            // Verify some invariants about the job info
            assert_eq!(b"default", job_info.cf_name().as_bytes());
            assert_eq!(0, job_info.cf_id());
            assert!(job_info.file_path().exists());

            // None of our tests are write intensive enough to trigger any write throttling
            assert!(!job_info.triggered_writes_slowdown());
            assert!(!job_info.triggered_writes_stop());

            self.flushes.fetch_add(1, Ordering::SeqCst);
            self.add_job_id(job_info.job_id());
        }

        fn compaction_completed(&self, job_info: &CompactionJobInfo) {
            // Verify some invariants about the job info
            assert_eq!(b"default", job_info.cf_name().as_bytes());
            assert_eq!(0, job_info.cf_id());

            // The input files to a compaction job will at some point be deleted.  However we can't
            // assume they're deleted at the time this event fires.
            //
            // Considerations on this are at
            // https://github.com/facebook/rocksdb/wiki/Delete-Stale-Files
            //
            // So for the input files, we still get the list of input files to exercise that
            // functionality in the code, but we can't make any assumptions about them.
            for file in job_info.input_files().into_iter() {
                assert!(file.exists() || !file.exists());
            }

            for file in job_info.output_files().into_iter() {
                assert!(file.exists());
            }

            self.compactions.fetch_add(1, Ordering::SeqCst);
            self.add_job_id(job_info.job_id());
        }
    }

    #[test]
    fn events_raised() -> Result<()> {
        let path = TempDBPath::new();
        let mut options = DBOptions::default();
        let listener = CountingTestListener::new();
        let flush_count = listener.flushes.clone();
        let compact_count = listener.compactions.clone();
        options.set_event_listener(listener);

        let db = DB::open(&path, options)?;
        let cf = db.get_cf("default").unwrap();

        // Write some data to there's somethign to flush
        crate::test::fill_db(&db, &cf, 1_000)?;

        assert_eq!(0, flush_count.load(Ordering::SeqCst));
        assert_eq!(0, compact_count.load(Ordering::SeqCst));

        db.flush(&cf, None)?;

        // The implementation of `flush` flushes memtables to disk, but will not trigger
        // compaction, so we expect one flush and no compactions
        assert_eq!(1, flush_count.load(Ordering::SeqCst));
        assert_eq!(0, compact_count.load(Ordering::SeqCst));

        // Write more data so there is something to flush and compact
        crate::test::fill_db(&db, &cf, 1_000)?;

        assert_eq!(1, flush_count.load(Ordering::SeqCst));
        assert_eq!(0, compact_count.load(Ordering::SeqCst));

        db.compact_all(&cf, None)?;

        // The implementation of `compact_all` first flushes, then does a compaction.  So this will
        // trigger both another flush, and a compaction
        assert_eq!(2, flush_count.load(Ordering::SeqCst));
        assert_eq!(1, compact_count.load(Ordering::SeqCst));

        Ok(())
    }

    #[test]
    fn job_info_marshalled() -> Result<()> {
        let path = TempDBPath::new();
        let mut options = DBOptions::default();
        let listener = CountingTestListener::new();
        let job_ids = listener.job_ids.clone();
        options.set_event_listener(listener);

        let db = DB::open(&path, options)?;
        let cf = db.get_cf("default").unwrap();

        // Write some data to there's somethign to flush
        crate::test::fill_db(&db, &cf, 1_000)?;

        db.compact_all(&cf, None)?;

        // There should have been one flush
        let job_ids = job_ids.lock().unwrap();

        assert!(job_ids.len() > 0);

        for job_id in job_ids.iter() {
            assert_ne!(0, *job_id);
        }

        Ok(())
    }
}
