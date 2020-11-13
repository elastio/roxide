//! Gets information about all of the "live" SST files in a database.
//!
//! A file is "live" if it contains active data.  SST files that exist on disk but are able to be
//! deleted because their contents have already been compacted into new SST files are not
//! considered "live", so we use the live files information to determine what files to store in S3.
use super::op_metrics;
use super::*;
use crate::db::DBLike;
use crate::ffi_util;
use crate::ops::get_db_ptr::GetDBPtr;
use crate::Result;
use std::path::PathBuf;

#[derive(Clone, Debug, PartialEq)]
pub struct SstFile {
    /// The name of the column family this file belongs to
    pub column_family_name: String,

    /// The level (currently from 1 to 6 inclusive) of this file.
    pub level: i32,

    /// The fully qualified path of this file
    pub file_path: PathBuf,

    /// The unique number of this file (also used to form the file name)
    pub file_number: u64,

    /// The size in bytes of the file on disk
    pub file_size: usize,
}

pub trait GetLiveFiles: RocksOpBase {
    /// Get information about all SST files which are live in this database at the moment this call
    /// is made.
    fn get_live_files(&self) -> Result<Vec<SstFile>>;
}

impl<DB> GetLiveFiles for DB
where
    DB: DBLike + GetDBPtr,
{
    fn get_live_files(&self) -> Result<Vec<SstFile>> {
        op_metrics::instrument_db_op(
            self,
            op_metrics::DatabaseOperation::GetLiveFiles,
            move || {
                let mut files = Vec::new();
                let files_vec_ptr: *mut Vec<SstFile> = &mut files;
                let db_ptr = self.get_db_ptr();

                unsafe {
                    cpp!([db_ptr as "rocksdb::DB*", files_vec_ptr as "void*"] {
                        std::vector<rocksdb::LiveFileMetaData> live_files;

                        db_ptr->GetLiveFilesMetaData(&live_files);

                        for (auto iterator=live_files.begin(); iterator != live_files.end(); iterator++) {
                            auto column_family_name_ptr = iterator->column_family_name.c_str();
                            auto level = iterator->level;
                            auto db_path = iterator->db_path.c_str();
                            auto file_name = iterator->name.c_str();
                            auto file_number = iterator->file_number;
                            auto file_size = iterator->size;

                            // For some reason, `file_name` always starts with a leading `/`, even though
                            // it's meant to be the path relative to db_path.  Fix that in C++ where
                            // anything goes.
                            if (file_name[0] == '/') {
                                file_name++;
                            }

                            rust!(GetLiveFiles_add_to_dev [
                                files_vec_ptr: *mut Vec<SstFile> as "void*",
                                column_family_name_ptr: *const libc::c_char as "const char*",
                                level: libc::c_int as "int",
                                db_path: *const libc::c_char as "const char*",
                                file_name: *const libc::c_char as "const char*",
                                file_number: u64 as "uint64_t",
                                file_size: libc::size_t as "size_t"
                            ] {
                                (*files_vec_ptr).push( SstFile {
                                    column_family_name: ffi_util::string_from_char_ptr(column_family_name_ptr),
                                    level: level as _,
                                    file_path: ffi_util::path_from_char_ptr(db_path).join(ffi_util::path_from_char_ptr(file_name)),
                                    file_number: file_number as _,
                                    file_size: file_size as _
                                })
                            });
                        }
                    });
                }

                Ok(files)
            },
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::db::*;
    use crate::db::{ColumnFamilyLike, DBLike};
    use crate::ops::Compact;
    use crate::test::TempDBPath;

    fn create_test_db() -> Result<(TempDBPath, DB, DBColumnFamily)> {
        let path = TempDBPath::new();
        let db = DB::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        Ok((path, db, cf))
    }

    #[test]
    fn get_live_files_empty_db() -> Result<()> {
        let (_path, db, _cf) = create_test_db()?;

        // When there is no data written to the DB, there are no live SST files
        assert_eq!(Vec::<SstFile>::new(), db.get_live_files()?);

        Ok(())
    }

    #[test]
    fn get_live_files_non_empty_db() -> Result<()> {
        let (path, db, cf) = create_test_db()?;

        crate::test::fill_db(&db, &cf, 10_000)?;
        db.compact_all(&cf, None)?;

        // There should be one SST file live, corresponding to this CF
        let live_files = db.get_live_files()?;
        assert_eq!(1, live_files.len());

        let sst = live_files.first().unwrap();

        assert_eq!(cf.name(), sst.column_family_name);
        assert!(
            sst.file_path.starts_with(path.path()),
            "Expected SST file path '{}' to start with DB path '{}'",
            sst.file_path.display(),
            path.path().display()
        );

        Ok(())
    }
}
