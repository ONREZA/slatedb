use super::Admin;
use crate::error::SlateDBError;
use futures::StreamExt;
use object_store::{path::Path, ObjectStoreExt};
use std::collections::BTreeSet;
use ulid::Ulid;

/// Caller-certified inputs for collecting an irrevocably retired, quiescent DB.
///
/// The caller must fence every writer, drain readers and unknown-outcome remote
/// I/O, and supply all live/pending clone and checkpoint SST roots. The library
/// cannot establish those facts from a DB's own manifest. `is_authorized` is
/// checked while listing and before each delete; the backend must independently
/// fence requests already in flight when authority expires.
pub struct RetiredSstGcOptions<'a> {
    pub protected: &'a BTreeSet<Path>,
    pub after: Option<&'a Path>,
    pub max_objects: usize,
    pub is_authorized: &'a (dyn Fn() -> bool + Send + Sync),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetiredSstGcResult {
    /// Resume strictly after this object, using a fresh protected-root union.
    /// A completed pass must eventually be followed by a new pass from `None`
    /// to catch objects published late by an unknown-outcome request.
    pub next: Option<Path>,
    pub examined: usize,
    pub deleted: usize,
}

impl Admin {
    /// Occupy the next immutable manifest version after external owner quiescence.
    /// A late unknown-outcome checkpoint PUT must either win before this CAS and
    /// be included in the caller's next root read, or lose its Create condition.
    /// This preserves writer epochs and all roots; it does not open a database.
    pub async fn fence_manifest_after_quiescence(&self) -> Result<Option<u64>, crate::Error> {
        use crate::manifest::store::StoredManifest;
        use slatedb_txn_obj::TransactionalObject;
        use std::sync::Arc;

        let Some(mut manifest) =
            StoredManifest::try_load(Arc::new(self.manifest_store()), self.system_clock.clone())
                .await?
        else {
            return Ok(None);
        };
        manifest
            .maybe_apply_update(|current| Ok(Some(current.prepare_dirty()?)))
            .await?;
        Ok(Some(manifest.id()))
    }

    /// Delete only unprotected, directly owned compacted SSTs after quiescence.
    ///
    /// Unlike normal GC, the retired DB's current tree and full parent-pinning
    /// checkpoints are not roots. This never touches WALs, manifests, parent
    /// checkpoints, unknown files, or another DB's compacted directory.
    ///
    /// Memory and deletes are bounded by `max_objects` (1..=1024). ObjectStore
    /// does not promise LIST order, so each chunk scans the remaining listing
    /// while retaining only its smallest `max_objects + 1` paths. The supplied
    /// backend must bound LIST duration and enforce its normal HTTP budget.
    /// On any error, retry the same cursor; successful earlier deletes are safe
    /// to repeat. No completion/cursor is returned for an ambiguous deletion.
    pub async fn collect_retired_ssts_after_quiescence(
        &self,
        options: RetiredSstGcOptions<'_>,
    ) -> Result<RetiredSstGcResult, crate::Error> {
        let dir = self.path.clone().join("compacted");
        if !(1..=1024).contains(&options.max_objects)
            || options.after.is_some_and(|path| !owned_sst(&dir, path))
        {
            return Err(SlateDBError::InvalidDBState.into());
        }
        let check = || {
            if (options.is_authorized)() {
                Ok(())
            } else {
                Err(crate::Error::from(SlateDBError::InvalidDBState))
            }
        };
        check()?;
        let store = self.retrying_store(crate::utils::ObjectStoreType::Main);
        // The retry wrapper buffers LIST results; stream here and retry failed chunks at the caller.
        let mut listing = match options.after {
            Some(after) => self.main_object_store.list_with_offset(Some(&dir), after),
            None => self.main_object_store.list(Some(&dir)),
        };
        let mut smallest = BTreeSet::new();
        loop {
            check()?;
            let Some(metadata) = listing.next().await else {
                break;
            };
            check()?;
            let path = metadata.map_err(SlateDBError::from)?.location;
            if owned_sst(&dir, &path) && options.after.is_none_or(|after| path > *after) {
                smallest.insert(path);
                if smallest.len() > options.max_objects + 1 {
                    smallest.pop_last();
                }
            }
        }
        let has_more = smallest.len() > options.max_objects;
        if has_more {
            smallest.pop_last();
        }
        let next = has_more.then(|| smallest.last().expect("nonempty bounded chunk").clone());
        let mut result = RetiredSstGcResult {
            next,
            examined: smallest.len(),
            deleted: 0,
        };
        for path in smallest {
            check()?;
            if options.protected.contains(&path) {
                continue;
            }
            match store.delete(&path).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => result.deleted += 1,
                Err(error) => return Err(SlateDBError::from(error).into()),
            }
            check()?;
        }
        Ok(result)
    }
}

fn owned_sst(dir: &Path, path: &Path) -> bool {
    let Some(mut parts) = path.prefix_match(dir) else {
        return false;
    };
    let Some(name) = parts.next() else {
        return false;
    };
    if parts.next().is_some() {
        return false;
    }
    name.as_ref()
        .strip_suffix(".sst")
        .is_some_and(|id| Ulid::from_string(id).is_ok())
}
