use borsh::BorshDeserialize;
use near_primitives::hash::CryptoHash;
use near_store::db::DBTransaction;
use near_store::metadata::DbKind;
use near_store::{DBCol, NodeStorage, Store};
use std::collections::HashSet;
use std::path::Path;

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
pub(crate) struct GCHeadersCommand {}

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

        let live_hashes = Self::load_all_live_blocks(&hot_store)?;
        let live_headers_set_transaction = Self::load_all_live_headers(&hot_store, &live_hashes)?;

        let mut delete_transaction = DBTransaction::new();
        delete_transaction.delete_all(DBCol::BlockHeader);
        hot_store.storage.write(delete_transaction)?;

        hot_store.storage.write(live_headers_set_transaction)?;

        Ok(())
    }

    fn load_all_live_headers(
        store: &Store,
        live_hashes: &HashSet<CryptoHash>,
    ) -> anyhow::Result<DBTransaction> {
        let mut transaction = DBTransaction::new();

        tracing::info!(target: "nearcore", "Start iterating BlockHeader");
        for result in store.iter(DBCol::BlockHeader) {
            let (key, value) = result?;
            let hash = CryptoHash::try_from_slice(&key)?;
            if live_hashes.contains(&hash) {
                transaction.set(DBCol::BlockHeader, key.to_vec(), value.to_vec());
            }
        }
        tracing::info!(target: "nearcore", "Finished iterating BlockHeader");

        Ok(transaction)
    }

    fn load_all_live_blocks(store: &Store) -> anyhow::Result<HashSet<CryptoHash>> {
        let mut live_hashes = HashSet::new();

        tracing::info!(target: "nearcore", "Start iterating BlockInfo");
        for result in store.iter(DBCol::BlockInfo) {
            let (key, _) = result?;
            live_hashes.insert(CryptoHash::try_from_slice(&key)?);
        }
        tracing::info!(target: "nearcore", "Finished iterating BlockInfo");

        Ok(live_hashes)
    }
}
