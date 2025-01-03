//! Gets information about all of the "live" SST files in a database.
//!
//! A file is "live" if it contains active data.  SST files that exist on disk but are able to be
//! deleted because their contents have already been compacted into new SST files are not
//! considered "live", so we use the live files information to determine what files to store in S3.
use super::op_metrics;
use super::*;
use crate::db::DbLike;
use crate::ffi_util;
use crate::ops::get_db_ptr::GetDbPtr;
use crate::Result;
use std::path::PathBuf;

#[derive(Clone, Debug, PartialEq, Eq)]
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

    /// The smallest database sequence number in the file
    pub smallest_seqno: u64,

    /// The largest database sequence number in the file
    pub largest_seqno: u64,

    /// The smallest key in the range of key/value pairs contained in the file
    pub smallest_key: Vec<u8>,

    /// The largest key in the range of key/value pairs contained in the file
    pub largest_key: Vec<u8>,

    /// An SST file may be generated by compactions whose input files may
    /// in turn be generated by earlier compactions. The creation time of the
    /// oldest SST file that is the compaction ancestor of this file.
    /// The timestamp is provided SystemClock::GetCurrentTime().
    /// 0 if the information is not available.
    ///
    /// Note: for TTL blob files, it contains the start of the expiration range.
    pub oldest_ancestor_time: u64,

    /// Timestamp when the SST file is created, provided by
    /// SystemClock::GetCurrentTime(). 0 if the information is not available.
    pub file_creation_time: u64,

    pub crc32c_checksum: u32,
}

pub trait GetLiveFiles: RocksOpBase {
    /// Get information about all SST files which are live in this database at the moment this call
    /// is made.
    fn get_live_files(&self) -> Result<Vec<SstFile>>;
}

impl<DB> GetLiveFiles for DB
where
    DB: DbLike + GetDbPtr,
{
    fn get_live_files(&self) -> Result<Vec<SstFile>> {
        op_metrics::instrument_db_op(
            self,
            op_metrics::DatabaseOperation::GetLiveFiles,
            move || {
                let db_ptr = self.get_db_ptr();
                unsafe { get_live_files_impl(db_ptr) }
            },
        )
    }
}

/// Given a pointer to a RocksDB C++ `DB` object, get the metadata for the live files actively in
/// use by the database.
///
/// We need a way to invoke this without a Rust `Db` instance to support
/// [`crate::Checkpoint::get_files`] which doesn't go through the Rust wrapper for maximum
/// performance.
pub(crate) unsafe fn get_live_files_impl(db_ptr: *mut libc::c_void) -> Result<Vec<SstFile>> {
    let mut files = Vec::new();
    let files_vec_ptr: *mut Vec<SstFile> = &mut files;

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
            auto smallest_seqno = iterator->smallest_seqno;
            auto largest_seqno = iterator->largest_seqno;
            auto smallest_key = iterator->smallestkey.c_str();
            auto smallest_key_len = iterator->smallestkey.size();
            auto largest_key = iterator->largestkey.c_str();
            auto largest_key_len = iterator->largestkey.size();
            auto oldest_ancester_time = iterator->oldest_ancester_time;
            auto file_creation_time = iterator->file_creation_time;

            // Default CRC32c checksum generator stores checksum inside std::string
            // using reinterpret_cast, so we can't convert it to rust string using
            // to_string_lossy(), because it will replace invalid UTF-8 characters
            uint32_t file_checksum_uint = iterator->file_checksum.length() == 4
                ? *reinterpret_cast<const uint32_t*>(iterator->file_checksum.data())
                : 0u;

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
                file_size: libc::size_t as "size_t",
                smallest_seqno: u64 as "uint64_t",
                largest_seqno: u64 as "uint64_t",
                smallest_key: *const libc::c_uchar as "const char*",
                smallest_key_len: libc::size_t as "size_t",
                largest_key: *const libc::c_uchar as "const char*",
                largest_key_len: libc::size_t as "size_t",
                oldest_ancester_time: u64 as "uint64_t",
                file_creation_time: u64 as "uint64_t",
                file_checksum_uint: u32 as "uint32_t"
            ] {
                (*files_vec_ptr).push( SstFile {
                    column_family_name: ffi_util::string_from_char_ptr(column_family_name_ptr),
                    level: level as _,
                    file_path: ffi_util::path_from_char_ptr(db_path).join(ffi_util::path_from_char_ptr(file_name)),
                    file_number: file_number as _,
                    file_size: file_size as _,
                    smallest_seqno: smallest_seqno as _,
                    largest_seqno: largest_seqno as _,
                    smallest_key: std::slice::from_raw_parts(smallest_key, smallest_key_len).to_vec(),
                    largest_key: std::slice::from_raw_parts(largest_key, largest_key_len).to_vec(),
                    oldest_ancestor_time: oldest_ancester_time as _,
                    file_creation_time: file_creation_time as _,
                    crc32c_checksum: file_checksum_uint as _
                })
            });
        }
    });

    Ok(files)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::db::*;
    use crate::db::{ColumnFamilyLike, DbLike};
    use crate::ops::Compact;
    use crate::test::TempDbPath;

    fn create_test_db() -> Result<(TempDbPath, Db, DbColumnFamily)> {
        let path = TempDbPath::new();
        let db = Db::open(&path, None)?;
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

        assert_eq!(cf.name().as_str(), sst.column_family_name.as_str());
        assert!(
            sst.file_path.starts_with(path.path()),
            "Expected SST file path '{}' to start with DB path '{}'",
            sst.file_path.display(),
            path.path().display()
        );

        Ok(())
    }

    /// A mostly exploratory test to confirm how the seqnum and key fields on SST metadata work
    #[test]
    fn get_live_files_seq_nums_and_keys() -> Result<()> {
        let (_path, db, cf) = create_test_db()?;

        // Write some data to the CF.  This should all fit in a single SST
        let start_seq_num = db.get_latest_sequence_number();
        crate::test::fill_db_deterministic(&db, &cf, 42, 10_000)?;
        let end_seq_num = db.get_latest_sequence_number();
        db.compact_all(&cf, None)?;

        // There should be one SST file live, corresponding to this CF
        let live_files = db.get_live_files()?;
        assert_eq!(1, live_files.len());

        let sst = live_files.first().unwrap();

        assert_eq!(cf.name().as_str(), sst.column_family_name.as_str());

        // This single SST should correspond to the full range of seq numbers
        // This starts at 1, not at 0, since at seq num 0 there were no SST files, the first write
        // operation went from 0 to 1, and that write is reflectedin the SST
        assert_eq!(start_seq_num + 1, sst.smallest_seqno);
        assert_eq!(end_seq_num, sst.largest_seqno);

        // Calculate the random keys generated by `fill_db_deterministic`, both the first and last
        let mut keys = crate::test::random_keys_deterministic(42, 10_000);
        keys.sort_unstable();
        let smallest = keys.first().unwrap();
        let largest = keys.last().unwrap();

        assert_eq!(*smallest, sst.smallest_key);
        assert_eq!(*largest, sst.largest_key);

        Ok(())
    }
}
