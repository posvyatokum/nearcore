use crate::config::ColumnBackupConfig;
use crate::db::{Database, RocksDB};
use crate::{
    DBCol, DBIterator, DBSlice, DBTransaction, Mode, StoreConfig, StoreStatistics, Temperature,
};
use std::collections::HashMap;
use std::io;
use std::sync::Arc;

pub type DBBackupsMap = HashMap<DBCol, Vec<Arc<dyn Database>>>;

pub struct DBWithBackups {
    main: Arc<dyn Database>,
    backups: DBBackupsMap,
}

impl DBWithBackups {
    pub fn new(
        main: Arc<dyn Database>,
        home_dir: &std::path::Path,
        backup_configs: Vec<ColumnBackupConfig>,
    ) -> Arc<Self> {
        return Arc::new(DBWithBackups {
            main,
            backups: Self::open_backups(home_dir, backup_configs),
        });
    }

    pub fn open_backups(
        home_dir: &std::path::Path,
        backup_configs: Vec<ColumnBackupConfig>,
    ) -> DBBackupsMap {
        let mut backups = DBBackupsMap::new();
        for backup_config in backup_configs.into_iter() {
            let column = backup_config.column;

            let mut store_config = StoreConfig::default();
            store_config.max_open_files = backup_config.max_open_files;
            store_config.block_size = backup_config.block_size;

            let path = home_dir.join(backup_config.path);

            let db = Arc::new(
                RocksDB::open(&path, &store_config, Mode::ReadOnly, Temperature::Hot)
                    .expect("Should be able to open every backup db"),
            );

            backups
                .entry(column)
                .and_modify(|dbs| dbs.push(db.clone()))
                .or_insert(vec![db.clone()]);
        }

        backups
    }
}

impl Database for DBWithBackups {
    fn get_raw_bytes(&self, col: DBCol, key: &[u8]) -> io::Result<Option<DBSlice<'_>>> {
        if let Some(main_result) = self.main.get_raw_bytes(col, key)? {
            return Ok(Some(main_result));
        }
        match self.backups.get(&col) {
            None => Ok(None),
            Some(backups) => {
                for backup in backups {
                    crate::metrics::BACKUP_STORE_READ_COUNT.with_label_values(&[col.into()]).inc();
                    if let Some(backup_result) = backup.get_raw_bytes(col, key)? {
                        return Ok(Some(backup_result));
                    }
                }
                Ok(None)
            }
        }
    }

    fn iter(&self, col: DBCol) -> DBIterator {
        self.main.iter(col)
    }

    fn iter_prefix<'a>(&'a self, col: DBCol, key_prefix: &'a [u8]) -> DBIterator {
        self.main.iter_prefix(col, key_prefix)
    }

    fn iter_range<'a>(
        &'a self,
        col: DBCol,
        lower_bound: Option<&[u8]>,
        upper_bound: Option<&[u8]>,
    ) -> DBIterator<'a> {
        self.main.iter_range(col, lower_bound, upper_bound)
    }

    fn iter_raw_bytes(&self, col: DBCol) -> DBIterator {
        self.main.iter_raw_bytes(col)
    }

    fn write(&self, transaction: DBTransaction) -> io::Result<()> {
        self.main.write(transaction)
    }

    fn flush(&self) -> io::Result<()> {
        self.main.flush()
    }

    fn compact(&self) -> io::Result<()> {
        self.main.compact()
    }

    /// Trying to get
    /// 1. RocksDB statistics
    /// 2. Selected RockdDB properties for column families
    fn get_store_statistics(&self) -> Option<StoreStatistics> {
        self.main.get_store_statistics()
    }

    fn create_checkpoint(
        &self,
        path: &std::path::Path,
        columns_to_keep: Option<&[DBCol]>,
    ) -> anyhow::Result<()> {
        self.main.create_checkpoint(path, columns_to_keep)
    }
}
