//! This module defines traits which aggregate all of the various operations defined in the `ops`
//! modules into a handful of traits to make it easier for clients to apply type constraints to get
//! the functionality they need without explicitly depending on concrete implementations like `DB`
//! or `Transaction`.

use super::{
    begin_tx::BeginTrans,
    checkpoint::Checkpoint,
    compact::Compact,
    delete::Delete,
    flush::Flush,
    get::Get,
    get_db_id::GetDBId,
    get_db_ptr::GetDBPtr,
    get_prop::*,
    iterate::IterateAll,
    merge::Merge,
    open::{DBOpen, DBOpenReadOnly},
    put::Put,
    stats::*,
    write::Write,
    RocksOpBase,
};
use crate::db::{db::*, opt_txdb::*, txdb::*};
use crate::tx::{sync, unsync};

/// All available data manipulation operations which are read-only and will not permute any data
pub trait DataReadOps: Get + IterateAll {}

/// All available data manipulation operations which add new data, or delete or permute existing
/// data
pub trait DataWriteOps: Delete + Merge + Put {}

/// Trait which aggregates all data manipulation operations, both read and write.  These are
/// supported on all database types as well as on transactions.
pub trait DataOps: DataReadOps + DataWriteOps {}

/// Trait which aggregates operations available on a read-only database.
pub trait ReadOnlyDBOps:
    DataReadOps + GetDBPtr + GetDBId + GetProperty + DBOpenReadOnly + Stats
{
}

/// Trait which aggregates all of the typical database operations with the exception of
/// `BeginTrans`. All of the database types implement this trait.
///
/// Note you might reasonably expect that `DBOps` is a subtrait of `ReadOnlyDBOps` but surprisingly
/// this is not the case.  The transaction-oriented database types actually do not suppport the
/// `DBOpenRead` operation.  Thus `ReadOnlyDBOps` and `DBOps` are separate traits, unrelated by
/// inheritance although obviously overlapping considerably
pub trait DBOps:
    DataOps + GetDBPtr + GetDBId + GetProperty + Stats + Checkpoint + Compact + Flush + DBOpen + Write
{
}

/// Trait which adds `BeginTrans` in addition to all operations exposed in the `DBOps` trait.
pub trait TransactionDBOps: DBOps + BeginTrans + Sync + Send
where
    <Self as RocksOpBase>::HandleType: Send,
{
}

impl DataReadOps for DB {}
impl DataWriteOps for DB {}
impl DataOps for DB {}
impl ReadOnlyDBOps for DB {}
impl DBOps for DB {}

impl DataReadOps for sync::Transaction {}
impl DataReadOps for unsync::Transaction {}
impl DataWriteOps for sync::Transaction {}
impl DataWriteOps for unsync::Transaction {}
impl DataOps for sync::Transaction {}
impl DataOps for unsync::Transaction {}

impl DataReadOps for TransactionDB {}
impl DataWriteOps for TransactionDB {}
impl DataOps for TransactionDB {}
impl DBOps for TransactionDB {}
impl TransactionDBOps for TransactionDB {}

impl DataReadOps for OptimisticTransactionDB {}
impl DataWriteOps for OptimisticTransactionDB {}
impl DataOps for OptimisticTransactionDB {}
impl DBOps for OptimisticTransactionDB {}
impl TransactionDBOps for OptimisticTransactionDB {}
