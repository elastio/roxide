//! Merge operators are a cool feature of Rocks, allowing each column family to specify an optional
//! operator which is applied when multiple values for the same key are encountered.  Normally the
//! newer value just replaces the old one, but merge operators can implement whatever logic they
//! need to.  All sorts of cool things are possible.

pub use rocksdb::merge_operator::{MergeFn, MergeOperands, MergeOperatorCallback};
