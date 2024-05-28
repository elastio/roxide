//! RocksDB's C++ API provides an ability for a caller to implement their own `Logger` class, to
//! capture log output from the RocksDB internals.  This is very helpful diagnostic information,
//! but it is unfortunately not exposed via the C bindings, and therefore not available in the
//! standard Rust bindings either.
//!
//! This module defines a C++ implementation of `Logger` which thunks to a Rust trait which
//! implements a similar `Logger` interface.  This allows Rust code to capture the Rocks log output
//! and direct it to whatever logging system the rest of the Rust system is using.

use crate::ffi_util::DynTraitWrapper;
use crate::labels::{
    ColumnFamilyLabels, ColumnFamilyOperationLabels, DatabaseLabels, DatabaseOperationLabels,
};
use cheburashka::labels::LabelSet;
use cheburashka::logging::futures::Instrument;
use cheburashka::logging::{prelude::*, AsSpan};
use core::future::Future;
use parking_lot::RwLock;
use std::any::Any;
use std::collections::HashMap;
use std::ffi;
use std::path::Path;
use std::sync::Arc;

// In the C++ source file which the cpp macro will generate make sure the relevant includes are
// present
cpp! {{
#include "src/lib.h"

using namespace rocksdb;

/// C++ implementation of the RocksDB `Logger` interface, which thunks the calls to a boxed
/// implementation of the Rust trait `Logger`.
class RustLogger : public rocksdb::Logger {
public:
    explicit RustLogger(const InfoLogLevel log_level, void* rust_boxed_logger)
        : rocksdb::Logger(log_level),
        rust_boxed_logger_(rust_boxed_logger)
    {}

    virtual ~RustLogger() {
        // Free the boxed logger
        rust!(RustLogger_free_boxed_logger [rust_boxed_logger_ : *mut ffi::c_void as "void*"] {
            unsafe {
                CppLoggerWrapper::free_raw_void(rust_boxed_logger_);
            }

        });

        rust_boxed_logger_ = nullptr;
    }

    // Returns the raw pointer to the inner Rust boxed logger.  Treat very carefully here, there
    // are a thousand footguns awaiting unwary callers.  In particular, do nothing with this
    // pointer that would cause Rust to free the boxed object inside, and do not assume this
    // pointer will outlive this `RustLogger` object.
    void * get_rust_boxed_logger() {
        return rust_boxed_logger_;
    }

    // Brings overloaded Logv()s into scope so they're not hidden when we override
    // a subset of them.
    using Logger::Logv;

    virtual void Logv(const char* format, va_list ap) override {
        // Assume this is logged at the INFO level
        Logv(rocksdb::InfoLogLevel::INFO_LEVEL, format, ap);
    }

    virtual void Logv(const rocksdb::InfoLogLevel log_level, const char* format, va_list ap) override {
        if (log_level < GetInfoLogLevel()) {
            return;
        }

        // Build this into a string.  No kidding this code is copied straight out of the RocksDb
        // source code.  Holy hard-coded buffer sizes!  The RocksDB code uses a hard-coded size
        // of 500; I made it double that, and if the buffer isn't big enough I just skip
        // logging.
        char msg[1024] = {0};
        int32_t n = vsnprintf(msg, sizeof(msg), format, ap);
        if (n > -1 && n < static_cast<int>(sizeof(msg))) {
            rust!(RustLogger_call_log [log_level: i32 as "int", msg: *const u8 as "char*", n: i32 as "int", rust_boxed_logger_: *mut ffi::c_void as "void*"] {
                unsafe {
                    CppLoggerWrapper::temp_from_raw_void(rust_boxed_logger_, |logger| {
                        logger.log(log_level, msg, n as usize);
                    });
                }
            });
        }
    }
private:
    void* rust_boxed_logger_;
};

}}

/// Given the pointer to an instance of the C++ `DB` class, reaches in to the options struct, and
/// gets the logger pointer.  If it's non-null, assumes that is is an instance of the `RustLogger`
/// C++ class above, and based on that assumption recovers the raw pointer of the Rust wrapper
/// object CppLoggerWrapper, and from there recovers the actual `RocksDbLogger` implementation,
/// passing it to a closure.
///
/// If this database doesn't have a configured logger, passes `None` to the closure.
pub(crate) unsafe fn temp_logger_from_raw_db_ptr<R, F>(db_ptr: *mut std::ffi::c_void, func: F) -> R
where
    F: FnOnce(Option<&(dyn RocksDbLogger + 'static)>) -> R + std::panic::UnwindSafe,
{
    let cpp_logger_ptr = cpp!([db_ptr as "rocksdb::DB*"] -> *mut std::ffi::c_void as "void*" {
        std::string id;
        auto options = db_ptr->GetDBOptions();
        auto logger_ptr = options.info_log;

        if (logger_ptr) {
            auto rust_logger = dynamic_cast<RustLogger*>(logger_ptr.get());
            if (rust_logger) {
                return rust_logger->get_rust_boxed_logger();
            }
        }

        return NULL;
    });

    if cpp_logger_ptr.is_null() {
        func(None)
    } else {
        CppLoggerWrapper::with_raw_boxed_logger(cpp_logger_ptr, |logger| func(Some(logger)))
    }
}

/// Gets the default logger impl to use, if the caller doesn't specify one
pub(crate) fn get_default_logger(db_path: &Path) -> (log::LevelFilter, impl RocksDbLogger) {
    (
        log::LevelFilter::Info,
        CheburashkaLogger::new(db_path.to_string_lossy()),
    )
}

static JSON_EVENT_PREFIX: &str = "EVENT_LOG_v1 ";

/// The context for a logger, which describes the fields to add to the logging system when
/// reporting events from RocksDB.
///
/// Note that there are some subtle but important differences in how metrics are labeled versus how
/// log contexts are labeled.  Because metrics are not associated with anything like a span which
/// can provide additional context, all information about a metric (meaning, all labels) must be
/// applied to that metric directly.  For example, a metric counting operations would have labels
/// from the database (path, id), the column family (name), and the operation (name).
///
/// When logging we have a more structured option, in which multiple nested spans can provide
/// additional context.  To continue the above example, when logging a message during an operation,
/// we first enter the database span (with path and id fields), then enter the column family span
/// (with the cf name field), and then the operation span (with the op name field).  Within this
/// span, we can log messages without providing any additional context, because all of the attached
/// spans will already carry that detail.
///
/// Therefore, when creating spans for a logging context, you'll see that not all labels are
/// actually used.  It's assumed that logging contexts will be nested appropriately.
///
/// `LoggingContext` is implemented on the various `LabelSet` types which are declared in the
/// [`labels`] module.  Because those types have a `LabelSet` and an `AsSpan` implementation, it's
/// easy to create logging spans containing all of the labels in the label set.  This approach
/// means that any changes to those label sets are automatically going to be reflected in the
/// corresponding logging context.
///
/// [`labels`]: roxide::labels
pub trait LoggingContext: LabelSet + AsSpan {
    /// Gets a copy of the fields in this context, in a `HashMap`.  This is mostly intended for
    /// testing.
    #[allow(dead_code)]
    fn fields(&self) -> HashMap<String, String> {
        // LabelSet already exposes all labels as a hash map.  Just need to convert the keys and
        // values into owned strings
        self.all_labels_as_map()
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.into_owned()))
            .collect()
    }

    /// Produces a `Span` in the Cheburashka logging system that captures
    /// all of the fields in this logging context.
    ///
    /// # Notes
    ///
    /// This is conceptually similar to `AsSpan::as_span`.  However it's not the same, and we don't
    /// use the `as_span` implementation here.  RocksDB logging is a bit different than elsewhere
    /// in the Elastio code base.  We stack spans instead of combining them.
    ///
    /// So for example, an event in the database context has only one span, "database", with fields
    /// showing the path and DB ID.  By contrast, a column family op is in the same database span,
    /// then then inside that in a column family span with the CF name, and inside that a CF op
    /// span with the op name.  This lets us group logs by span to either see all of the events for
    /// a database, or a CF, or to drill down all the way to the CF op level.
    ///
    /// This scheme was designed before the `LabelSet` and `AsSpan` traits were even conceived.  If
    /// this were to be built again now, it would not be done that way.
    fn span(&self) -> cheburashka::logging::Span;

    /// Run the specified closure within this logging context, such that all log events generated
    /// will be associated with the span(s) in this context.
    ///
    /// # Notes
    ///
    /// This is an exception to the general principle that log contexts do not contain all possible
    /// label fields.  This may enter multiple spans, depending upon what kind of metric label
    /// struct is being used.  For example, if this is a `ColumnFamilyOperationLabels`
    /// struct, then `in_context` first enters the database context, then the column family
    /// context, then the CF op context, and finally invokes the closure.
    fn in_context<R, F: FnOnce() -> R>(&self, func: F) -> R;

    /// Operates the same as `in_context`, but on async operations
    fn in_async_context<R, Fut: Future<Output = R>, F: FnOnce() -> Fut>(
        &self,
        func: F,
    ) -> cheburashka::logging::futures::Instrumented<Fut> {
        self.in_context(move || func().in_current_span())
    }
}

/// The context for log events coming from the RocksDB `DB` object.  Each of these fields is added
/// as context to log events generated by the RocksDB code.
impl<'a> LoggingContext for DatabaseLabels<'a> {
    fn span(&self) -> cheburashka::logging::Span {
        cheburashka::logging::info_span!(
            target: "rocksdb",
            "database",
            path = %self.path,
            db_id = %self.db_id
        )
    }

    fn in_context<R, F: FnOnce() -> R>(&self, func: F) -> R {
        let span = self.span();
        let _e = span.enter();

        func()
    }
}

/// Logging context for an operation performed at the database level
impl<'a> LoggingContext for DatabaseOperationLabels<'a> {
    fn span(&self) -> cheburashka::logging::Span {
        cheburashka::logging::info_span!(
            target: "rocksdb",
            "database_op",
            op_name = %self.op_name
        )
    }

    fn in_context<R, F: FnOnce() -> R>(&self, func: F) -> R {
        // First enter the database context, then the database op context
        self.db_labels.in_context(|| {
            let span = self.span();
            let _e = span.enter();

            func()
        })
    }
}

/// Logging context for a column family
impl<'a> LoggingContext for ColumnFamilyLabels<'a> {
    fn span(&self) -> cheburashka::logging::Span {
        cheburashka::logging::info_span!(
            target: "rocksdb",
            "column_family",
            cf_name = %self.cf_name
        )
    }

    fn in_context<R, F: FnOnce() -> R>(&self, func: F) -> R {
        // First enter the database context, then the cf context
        self.db_labels.in_context(|| {
            let span = self.span();
            let _e = span.enter();

            func()
        })
    }
}

/// Logging context for an operation performed at the column family level
impl<'a> LoggingContext for ColumnFamilyOperationLabels<'a> {
    fn span(&self) -> cheburashka::logging::Span {
        cheburashka::logging::info_span!(
            target: "rocksdb",
            "column_family_op",
            op_name = %self.op_name
        )
    }

    fn in_context<R, F: FnOnce() -> R>(&self, func: F) -> R {
        // First enter the database context, then the cf context, and finally the cf op context
        self.cf_labels.db_labels.in_context(|| {
            self.cf_labels.in_context(|| {
                let span = self.span();
                let _e = span.enter();

                func()
            })
        })
    }
}

/// Trait which is implemented in Rust but which is converted into a RocksDb `Logger` class
/// implementation to capture RocksDB log events in Rust
///
/// Note that the methods on this trait will be called directly from the RocksDB threads thus this
/// must be completely thread safe and should not lock or block for any length of time or there
/// will be serious performance consequences.
pub trait RocksDbLogger: Send + Sync + Any {
    /// If overridden to return `true`, then a JSON event is logged by RocksDB, it will be parsed
    /// and passed to `log_json_event`.  If false, these kinds of log messages are ignored.
    fn include_json_events(&self) -> bool {
        false
    }

    /// Writes a header line to the log.  By default this is just logged as any other message at
    /// the INFO level.  The string is opaque and we can't make any assumptions about it's
    /// structure so it seems pretty useless to make this distinction.
    fn log_header(&self, header: &[u8]) {
        self.log(log::Level::Info, header);
    }

    /// Logs a C string directly from the C++ code.  This is NOT a Rust string type.  Most
    /// implementations should use the default impl of this method and instead implement `log_str`
    /// for better ergonomics
    fn log(&self, level: log::Level, msg: &[u8]) {
        // This message may or may not be valid UTF-8.  `from_utf8_lossy` will only allocate a new
        // string if `msg` is not a valid UTF-8 string already, in which case any invalid bytes will
        // be dropped
        let msg = String::from_utf8_lossy(msg);

        // Inspite of the name "prefix", the JSON event prefix isn't always at the start of the log
        // message, because some JSON event messages are passed through another `Logger` which
        // prepends a string like "(Original Log Time ...)" for some reason.  So instead we assume
        // any log message that contains the prefix anywhere is a JSON event message
        if let Some(prefix_index) = msg.find(JSON_EVENT_PREFIX) {
            if self.include_json_events() {
                // Parse this message as JSON and pass it to the JSON event log handling method
                let (_, json_msg) = msg.split_at(prefix_index + JSON_EVENT_PREFIX.len());
                self.log_json_event(level, serde_json::from_str(json_msg));
            }
        } else {
            self.log_str(level, &msg);
        }
    }

    fn log_str(&self, level: log::Level, msg: &str);

    /// RocksDB internally has a `Logger` class which it uses to report on its internal events via
    /// the logging system.  While we have an `events` module that exposes some of these events to
    /// Rust, we don't support all of the event types or properties because we just don't need them
    /// yet.
    ///
    /// This method is called when an item is logged by this event logger.  `msg` in this case is a
    /// JSON string, containing various pieces of information about an event.
    ///
    /// If an implementation of this trait is interested in these log messages, override
    /// `include_json_events` to return `true`.
    fn log_json_event(&self, level: log::Level, msg: serde_json::Result<serde_json::Value>) {
        let _ = level;
        let _ = msg;
        unimplemented!()
    }

    /// Called on a logging implementation after the database it's attached to has been created,
    /// opened, whatever.  The intention is to add the information about the database or column
    /// family to which this logger pertains.
    ///
    /// Due to the idiosyncracies of the Rocks API design and also this wrapper impl, it's not
    /// possible to obtain the full context information at the time the logger is created; first
    /// the database needs to be opened, and THEN queried for the DB ID, which is part of the
    /// context.  Thus, loggers are initially created with a partial context, then filled out with
    /// the full context only after the open operation completes successfully.
    fn set_context(&self, context: DatabaseLabels);
}

/// An implementation of `RocksDbLogger` which passes log events to Cheburashka, our observability
/// crate.
///
/// This is the default logging implementation for our use cases.  It allows to update the `Span`
/// at runtime in response to a `set_context` call, which is how we're able to annotate our log
/// events with information about the database and column family the logger is associated with.
pub struct CheburashkaLogger {
    span: RwLock<cheburashka::logging::Span>,
}

impl CheburashkaLogger {
    /// Create a new CheburashkaLogger with an initial set of key/value pairs added as context to
    /// every log event.  This can be changed at runtime with a call to `set_context`.
    pub fn new(db_path: impl Into<String>) -> Self {
        // Create an initial bootstrap span.  This won't have all of the information about the
        // database because that requires the database be opened in order to recover.  This just
        // provides enough context to identify the log events associated with the initial open
        // process with the database path.  It's not ideal but it'll be sufficient for now.
        //
        // See the comment on `RocksDbLogger::set_context` for more details
        let span = cheburashka::logging::info_span!(target: "rocksdb",
            "db_bootstrap",
            db_path = %db_path.into());
        CheburashkaLogger {
            span: RwLock::new(span),
        }
    }
}

impl RocksDbLogger for CheburashkaLogger {
    fn include_json_events(&self) -> bool {
        true
    }

    // the complexity comes from the macro expansion, devs don't see it
    #[allow(clippy::cognitive_complexity)]
    fn log_str(&self, level: log::Level, msg: &str) {
        // Enter the logging span, then log the message to the logging system.
        let span = self.span.read();
        let _e = span.enter();

        // Translate into cheburasha's log level types
        let level = cheburashka::logging::log_level(level);

        // Because `level` here is non-const, we need to use the slightly less efficient
        // `dyn_event`
        cheburashka::logging::dyn_event!(
            target: "rocksdb_log_event",
            level,
            msg);
    }

    // The cognitive complexity doesn't impact readability in this case, IMHO
    #[allow(clippy::cognitive_complexity)]
    fn log_json_event(&self, level: log::Level, msg: serde_json::Result<serde_json::Value>) {
        // Enter the logging span, then log the message to the logging system.
        let span = self.span.read();
        let _e = span.enter();

        // Translate the log level to the Cheburashka concept of levels
        let level = cheburashka::logging::log_level(level);

        // If the JSON is invalid, log the error, otherwise log the pretty-printed JSON value
        //
        match msg {
            Ok(json_event) => match json_event {
                serde_json::Value::Object(json_event) => {
                    let event = json_event
                        .get("event")
                        .and_then(|e| e.as_str())
                        .unwrap_or("('event' field missing)");

                    let json_event = serde_json::to_string(&json_event)
                        .expect("JSON value serialization must be always infallible");

                    cheburashka::logging::dyn_event!(
                        target: "rocksdb_log_json_event",
                        level,
                        event,
                        json = json_event.as_str(),
                        "RocksDB {}",
                        event
                    );
                }
                json_event => {
                    cheburashka::logging::error!(target: "rocksdb_log_json_event",
                        json = %json_event,
                        "RocksDB event is not a JSON object");
                }
            },
            Err(e) => {
                cheburashka::logging::error!(target: "rocksdb_log_json_event",
                    error = log_error(&e),
                    "RocksDB reported an event but the JSON failed to parse"
                );
            }
        }
    }

    fn set_context(&self, context: DatabaseLabels) {
        // Replace the span with a new one created by this context
        let mut guard = self.span.write();

        let new_span = context.span();

        let _old_span = std::mem::replace(&mut *guard, new_span);
    }
}

make_dyn_trait_wrapper!(pub(crate) CppLoggerWrapper, RocksDbLogger);

impl CppLoggerWrapper {
    /// Presents a slightly more C-friendly interface on the `RocksDbLogger` trait, to make it easier
    /// to call from within C land.
    ///
    /// Decodes the level and calls either `log` or `log_header` on the wrapped implementation
    /// depending upon the log level
    unsafe fn log(&self, level: i32, msg: *const u8, length: usize) {
        let slice = std::slice::from_raw_parts(msg, length);

        if level == 5 {
            // InfoLogLevel::HEADER_LEVEL means log as a header line
            self.0.log_header(slice);
        } else {
            // Convert the level to the `log` crate levels.  Note that the rocksdb code
            // hard-codeds an array based on the values of the enum so I don't feel so
            // bad hard coding here
            let level = match level {
                0 => log::Level::Debug,
                1 => log::Level::Info,
                2 => log::Level::Warn,
                3 | 4 => log::Level::Error,
                _ => log::Level::Debug,
            };

            self.0.log(level, slice);
        }
    }

    /// Very dangerous helper that will take a pointer to the Rust boxed logger held by the C++
    /// class `RustLogger`, converts it back to a boxed `RocksDbLogger` (with no validation; if
    /// this pointer isn't valid you'll get nasty UBF), and passes that boxed logger to a closure.
    ///
    /// This closure can call methods on that logger but it cannot take ownership.
    pub(crate) unsafe fn with_raw_boxed_logger<R, F>(
        logger_ptr: *mut std::ffi::c_void,
        func: F,
    ) -> R
    where
        F: FnOnce(&(dyn RocksDbLogger + 'static)) -> R + std::panic::UnwindSafe,
    {
        Self::temp_from_raw_void(logger_ptr, move |wrapper| {
            wrapper.with_inner(|inner: &Arc<_>| {
                use std::ops::Deref;
                let logger: &(dyn RocksDbLogger + 'static) = inner.deref();

                func(logger)
            })
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::db::{Db, DbLike};
    use crate::db_options::DbOptions;
    use crate::ops::{Compact, DbOpen, Flush};
    use crate::test::TempDbPath;
    use crate::Result;
    use std::sync::{Arc, Mutex};

    pub(crate) struct TestLogger {
        pub include_json: bool,
        pub messages: Arc<Mutex<Vec<String>>>,
        pub json_events: Arc<Mutex<Vec<serde_json::Result<serde_json::Value>>>>,
        pub context: Arc<Mutex<HashMap<String, String>>>,
    }

    impl TestLogger {
        pub fn new() -> Self {
            TestLogger {
                include_json: false,
                messages: Arc::new(Mutex::new(Vec::<String>::new())),
                json_events: Arc::new(Mutex::new(
                    Vec::<serde_json::Result<serde_json::Value>>::new(),
                )),
                context: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        fn include_json(&mut self, include: bool) {
            self.include_json = include;
        }
    }

    impl RocksDbLogger for TestLogger {
        fn include_json_events(&self) -> bool {
            self.include_json
        }

        fn log_str(&self, _level: log::Level, msg: &str) {
            self.messages.lock().unwrap().push(msg.to_owned());
        }

        fn log_json_event(&self, _level: log::Level, msg: serde_json::Result<serde_json::Value>) {
            self.json_events.lock().unwrap().push(msg);
        }

        fn set_context(&self, new_context: DatabaseLabels) {
            let mut context = self.context.lock().unwrap();

            context.clear();
            context.extend(new_context.fields());
        }
    }

    #[test]
    fn test_rust_logger() -> Result<()> {
        let logger = TestLogger::new();

        let messages = logger.messages.clone();

        let mut options = DbOptions::default();
        options.set_logger(log::LevelFilter::Debug, logger);

        let path = TempDbPath::new();
        let db = Db::open(&path, options)?;
        let cf = db.get_cf("default").unwrap();

        crate::test::fill_db(&db, &cf, 1_000)?;

        // There should be messages logged as a result of all this
        // Use `clone` to operate on a copy of `messages`, so that test code is free to make assertions and panic
        // without poisoning our lock
        let results = messages.lock().unwrap().clone();
        assert_ne!(Vec::<String>::new(), results);

        assert!(
            results
                .iter()
                .any(|msg: &String| msg.contains("[db/version_set.cc:")),
            "The messages did not contain a line that is always present at DB startup: \n{}",
            results.join("\n")
        );

        Ok(())
    }

    #[test]
    fn json_events_parsed() -> Result<()> {
        let mut logger = TestLogger::new();
        logger.include_json(true);
        let json_events = logger.json_events.clone();
        let messages = logger.messages.clone();

        let mut options = DbOptions::default();
        options.set_logger(log::LevelFilter::Trace, logger);

        let path = TempDbPath::new();
        let db = Db::open(&path, options)?;
        let cf = db.get_cf("default").unwrap();

        // Write the same keys but different random values multiple times
        // That will require the compaction process to do more work
        crate::test::fill_db_deterministic(&db, &cf, 0xdeadbeef, 1_000)?;

        // Flushing will write the test data to a disk file.  That file will need to be merged with
        // the data we're going to write next; that merging process is compaction
        db.flush(&cf, None)?;

        crate::test::fill_db_deterministic(&db, &cf, 0xdeadbeef, 1_000)?;

        db.compact_all(&cf, None)?;

        // At the very least there should be one compact and one flush event, possibly others
        // Use `clone()` to operate on a copy of `json_events`, so that test code is free to make assertions and panic
        // without poisoning our lock
        let results: Vec<_> = json_events
            .lock()
            .unwrap()
            .iter()
            .map(|r| match r {
                Err(e) => Err(e.to_string()),
                Ok(j) => Ok(j.clone()),
            })
            .collect();

        for message in messages.lock().unwrap().iter() {
            dbg!(message);
        }

        let mut found_flush = false;
        let mut found_compact = false;
        for json_event in results.iter() {
            // The logging code doesn't require that the JSON msg is valid; it gives us the `Result`
            // object.  But for testing purposes we will require that all events parse
            match json_event {
                Err(e) => panic!("Failed to parse JSON event: {}", e),
                Ok(json_event) => {
                    dbg!(json_event.to_string());
                    match json_event {
                        serde_json::Value::Object(json_event) => {
                            // All events have a time stamp and an event type
                            assert_ne!(None, json_event.get("time_micros"));
                            assert_ne!(None, json_event.get("event"));

                            let event = dbg!(json_event.get("event").unwrap().as_str().unwrap());

                            if event == "flush_finished" {
                                found_flush = true;
                            } else if event == "compaction_finished" {
                                found_compact = true;
                            }
                        }
                        other => {
                            panic!("Expected object, found {}", other);
                        }
                    };
                }
            }
        }

        assert!(found_flush);
        assert!(found_compact);

        Ok(())
    }
}
