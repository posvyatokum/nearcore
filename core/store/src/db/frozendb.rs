use crate::db::{DBIterator, DBSlice, DBTransaction, Database};
use crate::DBCol;

/// Frozen DB has no restrictions on columns.
/// It can store all columns.
pub struct FrozenDB {
    frozen: std::sync::Arc<dyn Database>,
}

impl FrozenDB {
    pub fn new(frozen: std::sync::Arc<dyn Database>) -> Self {
        Self { frozen }
    }
}

impl Database for FrozenDB {
    /// Returns raw bytes for given `key` ignoring any reference count decoding if any.
    fn get_raw_bytes(&self, col: DBCol, key: &[u8]) -> std::io::Result<Option<DBSlice<'_>>> {
        self.frozen.get_raw_bytes(col, key)
    }

    /// Returns value for given `key` forcing a reference count decoding.
    fn get_with_rc_stripped(&self, col: DBCol, key: &[u8]) -> std::io::Result<Option<DBSlice<'_>>> {
        self.frozen.get_with_rc_stripped(col, key)
    }

    /// Iterates over all values in a column.
    fn iter<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        self.frozen.iter(col)
    }

    /// Iterates over values in a given column whose key has given prefix.
    fn iter_prefix<'a>(&'a self, col: DBCol, key_prefix: &'a [u8]) -> DBIterator<'a> {
        self.frozen.iter_prefix(col, key_prefix)
    }

    /// Iterate over items in given column bypassing reference count decoding if any.
    fn iter_raw_bytes<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        self.frozen.iter_raw_bytes(col)
    }

    /// Iterate over items in given column whose keys are between [lower_bound, upper_bound)
    fn iter_range<'a>(
        &'a self,
        col: DBCol,
        lower_bound: Option<&[u8]>,
        upper_bound: Option<&[u8]>,
    ) -> DBIterator<'a> {
        self.frozen.iter_range(col, lower_bound, upper_bound)
    }

    /// Atomically applies operations in given transaction.
    ///
    /// If debug assertions are enabled, panics if there are any delete
    /// operations or operations decreasing reference count of a value.  If
    /// debug assertions are not enabled, such operations are filtered out.
    fn write(&self, _transaction: DBTransaction) -> std::io::Result<()> {
        return Err(std::io::Error::other(format!("Frozen storage is read-only.")));
    }

    fn compact(&self) -> std::io::Result<()> {
        self.frozen.compact()
    }

    fn flush(&self) -> std::io::Result<()> {
        self.frozen.flush()
    }

    fn get_store_statistics(&self) -> Option<crate::StoreStatistics> {
        self.frozen.get_store_statistics()
    }

    fn create_checkpoint(
        &self,
        path: &std::path::Path,
        columns_to_keep: Option<&[DBCol]>,
    ) -> anyhow::Result<()> {
        self.frozen.create_checkpoint(path, columns_to_keep)
    }
}
