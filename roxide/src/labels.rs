//! Contains the [`LabelSet`] implementations that label log events and metrics related to RocksDB
//! databases, column families, and operations thereon.
//!
//! [`LabelSet`]: cheburashka::labels::LabelSet

// This lint is triggering for the `path` and `db_id` members of `DatabaseLabels`, and
// putting this `allow` attribute at the struct or field level doesn't disable the warning
//
// There's a known bug (https://github.com/rust-lang/rust-clippy/issues/2944) around this lint and
// it seems to be of minimal value anyway
#![allow(clippy::needless_lifetimes)]

use crate::db::ColumnFamilyLike;
use crate::db::DBLike;
use cheburashka::labels::LabelSet;
use cheburashka::logging::AsSpan;

pub fn get_db_metric_labels(db: &impl crate::db::DBLike) -> DatabaseLabels {
    DatabaseLabels {
        path: db.path_str(),
        db_id: db.db_id(),
    }
}

pub fn get_db_metric_labels_for_tx(tx: &crate::tx::unsync::Transaction) -> DatabaseLabels {
    DatabaseLabels {
        path: tx.db_path_str(),
        db_id: tx.db_id(),
    }
}

pub fn get_cf_metric_labels(cf: &impl crate::db::ColumnFamilyLike) -> ColumnFamilyLabels {
    ColumnFamilyLabels {
        db_labels: DatabaseLabels {
            path: cf.db_path_str(),
            db_id: cf.db_id(),
        },
        cf_name: cf.name(),
    }
}

/// Holds the labels that describe a database.  All database metrics are labeled with these labels at
/// a minimum
///
/// # Notes
///
/// *IMPORTANT* If any fields are added to the metrics labels, they must also be added to the
/// logging context.  See the `logging` module's implementation of `LoggingContext`.
#[derive(Debug, Clone, LabelSet, AsSpan)]
#[span(target = "rocksdb", name = "database")]
pub struct DatabaseLabels<'a> {
    #[label(tag, rename = "db_path")]
    pub path: &'a str,

    #[label(tag)]
    pub db_id: &'a str,
}

impl<'a, DB: DBLike> From<&'a DB> for DatabaseLabels<'a> {
    fn from(db: &'a DB) -> Self {
        DatabaseLabels {
            path: db.path_str(),
            db_id: db.db_id(),
        }
    }
}

/// Holds the labels that describe a database operation.  All database operation metrics are labeled with these labels at
/// a minimum
///
/// # Notes
///
/// *IMPORTANT* If any fields are added to the metrics labels, they must also be added to the
/// logging context.  See the `logging` module's implementation of `LoggingContext`.
#[derive(Debug, Clone, LabelSet, AsSpan)]
#[span(target = "rocksdb", name = "database_op")]
#[label_set(
    tag_expression(db_path = "self.db_labels.path"),
    tag_expression(db_id = "self.db_labels.db_id")
)]
pub struct DatabaseOperationLabels<'a> {
    /// This field isn't used as a label directly; rather its own labels are exposed via label
    /// expressions
    #[label(ignore)]
    pub db_labels: DatabaseLabels<'a>,

    #[label(tag)]
    pub op_name: &'static str,
}

impl<'a> DatabaseOperationLabels<'a> {
    pub fn new(db: &'a impl DBLike, op_name: &'static str) -> Self {
        DatabaseOperationLabels {
            db_labels: db.into(),
            op_name,
        }
    }

    /// Create a new metrics label from a transaction instead of directly from a database
    pub fn new_from_tx(tx: &'a crate::tx::unsync::Transaction, op_name: &'static str) -> Self {
        DatabaseOperationLabels {
            db_labels: DatabaseLabels {
                path: tx.db_path_str(),
                db_id: tx.db_id(),
            },
            op_name,
        }
    }
}

/// Holds the labels that describe a column family (aka table) in a database.  All column family
/// metrics are labeled with these labels at a minimum
///
/// # Notes
///
/// *IMPORTANT* If any fields are added to the metrics labels, they must also be added to the
/// logging context.  See the `logging` module's implementation of `LoggingContext`.
#[derive(Debug, Clone, LabelSet, AsSpan)]
#[span(target = "rocksdb", name = "column_family")]
#[label_set(
    tag_expression(db_path = "self.db_labels.path"),
    tag_expression(db_id = "self.db_labels.db_id")
)]
pub struct ColumnFamilyLabels<'a> {
    /// This field isn't used as a label directly; rather its own labels are exposed via label
    /// expressions
    #[label(ignore)]
    pub db_labels: DatabaseLabels<'a>,

    #[label(tag)]
    pub cf_name: &'a str,
}

impl<'a, CF: ColumnFamilyLike> From<&'a CF> for ColumnFamilyLabels<'a> {
    fn from(cf: &'a CF) -> Self {
        ColumnFamilyLabels {
            db_labels: DatabaseLabels {
                path: cf.db_path_str(),
                db_id: cf.db_id(),
            },
            cf_name: cf.name(),
        }
    }
}

/// Holds the labels that describe an operation on a column family.
///
/// # Notes
///
/// *IMPORTANT* If any fields are added to the metrics labels, they must also be added to the
/// logging context.  See the `logging` module's implementation of `LoggingContext`.
#[derive(Debug, Clone, LabelSet, AsSpan)]
#[span(target = "rocksdb", name = "column_family_op")]
#[label_set(
    tag_expression(db_path = "self.cf_labels.db_labels.path"),
    tag_expression(db_id = "self.cf_labels.db_labels.db_id")
    tag_expression(cf_name = "self.cf_labels.cf_name")
)]
pub struct ColumnFamilyOperationLabels<'a> {
    /// This field isn't used as a label directly; rather its own labels are exposed via label
    /// expressions
    #[label(ignore)]
    pub cf_labels: ColumnFamilyLabels<'a>,

    #[label(tag)]
    pub op_name: &'static str,
}

impl<'a> ColumnFamilyOperationLabels<'a> {
    pub fn new(cf: &'a impl ColumnFamilyLike, op_name: &'static str) -> Self {
        ColumnFamilyOperationLabels {
            cf_labels: cf.into(),
            op_name,
        }
    }
}
