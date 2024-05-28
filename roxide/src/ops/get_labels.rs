//! We use metrics labels (defined in the `metrics` module) to provide context to both metrics and
//! log events.  By using the same labels for both, our reporting tools are able to correlate log
//! events with particular metrics values, thereby making it much easier to understand the behavior
//! of a running system.
//!
//! These traits are implemented by types that contain the context required to produce the labels.
//! It allows the ops code to remain generic, without tight coupling to the metrics logic for each
//! different type of object (db, CF, etc).
use crate::db::ColumnFamilyLike;
use crate::db::DbLike;
use crate::labels;

#[allow(dead_code)]
pub(crate) trait GetDbLabels {
    fn get_db_labels(&self) -> labels::DatabaseLabels;
    fn get_db_op_labels(&self, op_name: &'static str) -> labels::DatabaseOperationLabels;
}

#[allow(dead_code)]
pub(crate) trait GetColumnFamilyLabels {
    fn get_cf_labels(&self) -> labels::ColumnFamilyLabels;
    fn get_cf_op_labels(&self, op_name: &'static str) -> labels::ColumnFamilyOperationLabels;
}

impl<T: DbLike> GetDbLabels for T {
    fn get_db_labels(&self) -> labels::DatabaseLabels {
        // DatabaseLabels has a `From` impl for `DBLike`
        self.into()
    }

    fn get_db_op_labels(&self, op_name: &'static str) -> labels::DatabaseOperationLabels {
        labels::DatabaseOperationLabels::new(self, op_name)
    }
}

impl GetDbLabels for crate::tx::unsync::Transaction {
    fn get_db_labels(&self) -> labels::DatabaseLabels {
        labels::get_db_metric_labels_for_tx(self)
    }

    fn get_db_op_labels(&self, _op_name: &'static str) -> labels::DatabaseOperationLabels {
        // There are no database-level ops that would ever be performed on a `Transaction`.  Any
        // attempt to do so is a bug.
        //
        // The database-level ops are `begin_trans`, `open`, and `checkpoint`.  None of those make
        // any sense as applied to a transaction object.
        unreachable!()
    }
}

impl<CF: ColumnFamilyLike> GetColumnFamilyLabels for CF {
    fn get_cf_labels(&self) -> labels::ColumnFamilyLabels {
        // ColumnFamilyLabels has a `From` impl for `ColumnFamilyLike`
        self.into()
    }

    fn get_cf_op_labels(&self, op_name: &'static str) -> labels::ColumnFamilyOperationLabels {
        labels::ColumnFamilyOperationLabels::new(self, op_name)
    }
}
