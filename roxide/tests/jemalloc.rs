//! Integration test to verify that Rocks is using jemalloc and not the normal allocator

#[cfg(feature = "jemalloc")]
mod jemalloc_enabled {
    use jemalloc_ctl::{epoch, stats};
    use more_asserts::*;
    use roxide::Result;
    use roxide::TempDbPath;
    use roxide::{self, BeginTrans, Cache, DbLike, DbOpen, Get, Put, TransactionDb};

    /// Allocate a block cache; that should allocate some memory in jemalloc
    #[test]
    fn allocate_cache() -> Result<()> {
        println!("Starting epoch: {}", epoch::advance().unwrap());

        let allocated = stats::allocated::read().unwrap();
        let resident = stats::resident::read().unwrap();
        println!(
            "Start of test: {} bytes allocated/{} bytes resident",
            allocated, resident
        );

        let _cache = Cache::with_capacity(2_000_000_000)?;

        println!("Ending epoch: {}", epoch::advance().unwrap());
        let end_allocated = stats::allocated::read().unwrap();
        let end_resident = stats::resident::read().unwrap();
        println!(
            "End of test: {} bytes allocated/{} bytes resident",
            end_allocated, end_resident
        );

        assert_gt!(end_allocated, allocated);
        Ok(())
    }

    /// Do some getting and putting on a live database and expect some jemalloc memory to be
    /// allocated
    #[test]
    fn db_get_put() -> Result<()> {
        println!("Starting epoch: {}", epoch::advance().unwrap());

        let allocated = stats::allocated::read().unwrap();
        let resident = stats::resident::read().unwrap();
        println!(
            "Start of test: {} bytes allocated/{} bytes resident",
            allocated, resident
        );

        let path = TempDbPath::new();
        let db = TransactionDb::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        let tx = db.begin_trans(None, None)?;

        let keys: Vec<_> = (0..1000).map(|n| format!("key{}", n)).collect();
        let num_keys = keys.len();

        let results = tx.multi_get(&cf, &keys, None)?;
        assert_eq!(num_keys, results.len());

        // None of these keys exist yet, so expect them all to be None
        for (_key, value) in results.into_iter() {
            assert_eq!(None, value);
        }

        // Now write every even-numbered key to the database
        let put_pairs: Vec<_> = (0..1000u32)
            .filter(|n| n % 2 == 0)
            .map(|n| (format!("key{}", n), format!("value{}", n)))
            .collect();

        for (k, v) in put_pairs.iter() {
            tx.put(&cf, k, v, None)?;
        }

        let results = tx.multi_get(&cf, &keys, None)?;
        assert_eq!(keys.len(), results.len());

        for (idx, (_key, value)) in results.into_iter().enumerate() {
            if idx % 2 == 0 {
                assert_eq!(format!("value{}", idx), value.unwrap().to_string_lossy());
            } else {
                assert_eq!(None, value);
            }
        }

        tx.commit()?;

        let results = db.multi_get(&cf, &keys, None)?;
        assert_eq!(keys.len(), results.len());

        for (idx, (_key, value)) in results.into_iter().enumerate() {
            if idx % 2 == 0 {
                assert_eq!(format!("value{}", idx), value.unwrap().to_string_lossy());
            } else {
                assert_eq!(None, value);
            }
        }

        println!("Ending epoch: {}", epoch::advance().unwrap());
        let end_allocated = stats::allocated::read().unwrap();
        let end_resident = stats::resident::read().unwrap();
        println!(
            "End of test: {} bytes allocated/{} bytes resident",
            end_allocated, end_resident
        );

        assert_gt!(end_allocated, allocated);
        Ok(())
    }
}

#[cfg(not(feature = "jemalloc"))]
mod jemalloc_disabled {
    #[test]
    fn jemalloc_disabled() {
        // mission accomplished
    }
}
