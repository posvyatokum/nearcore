use near_store::db::DBTransaction;
use near_store::metadata::DbKind;
use near_store::{DBCol, NodeStorage, Store};
use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;

/// This can potentially support db specified not in config, but in command line.
/// `ChangeRelative { path: Path, archive: bool }`
/// But it is a pain to implement, because of all the current storage possibilities.
/// So, I'll leave it as a TODO(posvyatokum): implement relative path DbSelector.
/// This can be useful workaround for config modification.
#[derive(clap::Subcommand)]
enum DbSelector {
    ChangeHot,
    ChangeCold,
}

#[derive(clap::Args)]
pub(crate) struct ChangeDbKindCommand {
    /// Desired DbKind.
    #[clap(long)]
    new_kind: DbKind,
    /// Which db to change.
    #[clap(subcommand)]
    db_selector: DbSelector,
}

impl ChangeDbKindCommand {
    pub(crate) fn run(&self, home_dir: &Path) -> anyhow::Result<()> {
        let near_config = nearcore::config::load_config(
            &home_dir,
            near_chain_configs::GenesisValidationMode::UnsafeFast,
        )?;
        let opener = NodeStorage::opener(
            home_dir,
            near_config.config.archive,
            &near_config.config.store,
            near_config.config.cold_store.as_ref(),
        );

        let storage = opener.open()?;
        let store = match self.db_selector {
            DbSelector::ChangeHot => storage.get_hot_store(),
            DbSelector::ChangeCold => {
                storage.get_cold_store().ok_or_else(|| anyhow::anyhow!("No cold store"))?
            }
        };
        Ok(store.set_db_kind(self.new_kind)?)
    }
}

#[derive(clap::Args)]
pub(crate) struct GCHeadersCommand {
    #[clap(long)]
    backup_home: PathBuf,
}

impl GCHeadersCommand {
    pub(crate) fn run(&self, home_dir: &Path) -> anyhow::Result<()> {
        let near_config = nearcore::config::load_config(
            &home_dir,
            near_chain_configs::GenesisValidationMode::UnsafeFast,
        )?;
        let opener = NodeStorage::opener(
            home_dir,
            near_config.config.archive,
            &near_config.config.store,
            near_config.config.cold_store.as_ref(),
        );
        let hot_store = opener.open()?.get_hot_store();

        let near_config = nearcore::config::load_config(
            &self.backup_home,
            near_chain_configs::GenesisValidationMode::UnsafeFast,
        )?;
        let opener = NodeStorage::opener(
            &self.backup_home,
            near_config.config.archive,
            &near_config.config.store,
            near_config.config.cold_store.as_ref(),
        );
        let backup_store = opener.open()?.get_hot_store();

        let live_hashes = Self::load_all_live_blocks(&hot_store)?;
        tracing::info!(target: "nearcore", "Collected live hashes, expect size around 216000, actual size {}", live_hashes.len());

        let mut delete_transaction = DBTransaction::new();
        delete_transaction.delete_all(DBCol::BlockHeader);
        hot_store.storage.write(delete_transaction)?;

        Self::write_all_live_headers(&hot_store, &backup_store, &live_hashes)?;
        Ok(())
    }

    fn write_all_live_headers(
        hot_store: &Store,
        backup_store: &Store,
        live_hashes: &HashSet<Vec<u8>>,
    ) -> anyhow::Result<()> {
        let mut transaction = DBTransaction::new();

        tracing::info!(target: "nearcore", "Start iterating BlockHeader");
        let mut counter = 0;
        for result in backup_store.iter(DBCol::BlockHeader) {
            let (key, value) = result?;
            let key_vec = key.to_vec();
            if live_hashes.contains(&key_vec) {
                transaction.set(DBCol::BlockHeader, key_vec, value.to_vec());
                counter += 1
            }

            if counter >= 5000 {
                let moved_transaction = std::mem::take(&mut transaction);
                hot_store.storage.write(moved_transaction)?;
                tracing::info!(target: "nearcore", ?counter, "Wrote transaction");
                counter = 0;
            }
        }
        tracing::info!(target: "nearcore", ?counter, "Wrote transaction");
        hot_store.storage.write(transaction)?;
        tracing::info!(target: "nearcore", "Finished iterating BlockHeader");

        Ok(())
    }

    fn load_all_live_blocks(store: &Store) -> anyhow::Result<HashSet<Vec<u8>>> {
        let mut live_hashes = HashSet::new();

        tracing::info!(target: "nearcore", "Start iterating Block");
        for result in store.iter(DBCol::Block) {
            let (key, _) = result?;
            live_hashes.insert(key.to_vec());
        }
        tracing::info!(target: "nearcore", "Finished iterating Block");

        Ok(live_hashes)
    }
}
