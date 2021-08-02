use crate::{GetLiveFiles, TransactionDbHandle};

pub type Result<T, E = crate::Error> = std::result::Result<T, E>;

pub trait ColumnFamilyCompactionTrigger {
    fn condition(&self) -> Result<bool>;
}

pub struct ColumnFamilyCompactionTriggerImp {
    db: Box<dyn GetLiveFiles<HandleType = TransactionDbHandle>>,
    column_family_name: String,
    number_of_files: usize,
}

impl ColumnFamilyCompactionTrigger for ColumnFamilyCompactionTriggerImp {
    fn condition(&self) -> Result<bool> {
        let live_files = self.db.get_live_files()?;
        let count = live_files
            .iter()
            .filter_map(|f| {
                if f.column_family_name == self.column_family_name {
                    Some(f)
                } else {
                    None
                }
            })
            .count();

        Ok(count > self.number_of_files)
    }
}

impl ColumnFamilyCompactionTriggerImp {
    pub fn new(
        db: Box<dyn GetLiveFiles<HandleType = TransactionDbHandle>>,
        cf_name: String,
        number_of_files: usize,
    ) -> Self {
        Self {
            db,
            number_of_files,
            column_family_name: cf_name,
        }
    }
}

#[cfg(test)]
pub mod tests {
    use crate::maintenance::ColumnFamilyCompactionTrigger;
    use crate::maintenance::ColumnFamilyCompactionTriggerImp;
    use crate::{DbLike, DbOpen, Flush, GetLiveFiles, RocksOpBase, SstFile, TransactionDbHandle};
    use crate::{DbOptions, TempDbPath, TransactionDb};
    use std::path::PathBuf;

    pub type Result<T, E = crate::Error> = std::result::Result<T, E>;

    #[test]
    fn file_num_compaction_trigger_returns_ok() -> super::Result<()> {
        let path = TempDbPath::new();
        let mut options = DbOptions::default();
        options.add_column_family("test_cf");
        let db = TransactionDb::open(&path, options)?;
        let cf = db.get_cf("test_cf").unwrap();

        crate::test::fill_db(&db, &cf, 50)?;

        db.flush_all(None)?;

        let trigger =
            ColumnFamilyCompactionTriggerImp::new(Box::new(db), String::from("test_cf"), 0);
        assert!(trigger.condition().is_ok());
        Ok(())
    }

    #[test]
    fn file_num_compaction_trigger_condition_true() -> super::Result<()> {
        let mut sst_files = Vec::new();

        for n in 1..21 {
            sst_files.push(SstFile {
                file_creation_time: 0,
                oldest_ancestor_time: 0,
                column_family_name: String::from("test_cf"),
                crc32c_checksum: 0,
                file_number: n,
                file_path: PathBuf::new(),
                file_size: 0,
                level: 0,
            });
        }

        let db = FakeDb::new(sst_files);

        let trigger =
            ColumnFamilyCompactionTriggerImp::new(Box::new(db), String::from("test_cf"), 20);
        assert!(trigger.condition().unwrap());
        Ok(())
    }

    struct FakeDb {
        live_files: Vec<SstFile>,
    }

    impl FakeDb {
        pub fn new(live_files: Vec<SstFile>) -> Self {
            Self { live_files }
        }
    }

    impl RocksOpBase for FakeDb {
        type HandleType = TransactionDbHandle;

        fn handle(&self) -> &Self::HandleType {
            todo!()
        }
    }

    impl GetLiveFiles for FakeDb {
        fn get_live_files(&self) -> Result<Vec<SstFile>> {
            Ok(self.live_files.clone())
        }
    }
}
