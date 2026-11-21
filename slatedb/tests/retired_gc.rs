use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use slatedb::admin::{AdminBuilder, RetiredSstGcOptions};
use slatedb::config::{CheckpointOptions, Settings};
use slatedb::object_store::{memory::InMemory, path::Path, ObjectStore, ObjectStoreExt};
use slatedb::object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use slatedb::Db;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use ulid::Ulid;

fn sst_path(directory: &str, id: u128) -> Path {
    Path::from(format!("{directory}/{}.sst", Ulid::from(id)))
}

async fn paths(store: &dyn ObjectStore) -> BTreeSet<Path> {
    store
        .list(None)
        .map_ok(|object| object.location)
        .try_collect()
        .await
        .unwrap()
}

#[tokio::test]
async fn retired_gc_paginates_owned_ssts_and_preserves_protected_and_foreign_objects() {
    let store = Arc::new(InMemory::new());
    let owned: Vec<_> = (1..=5)
        .map(|id| sst_path("retired/compacted", id))
        .collect();
    let protected = BTreeSet::from([owned[0].clone(), owned[3].clone()]);
    let foreign = BTreeSet::from([
        sst_path("other/compacted", 1),
        sst_path("retired/compacted/nested", 1),
        sst_path("retired/wal", 1),
        Path::from("retired/compacted/not-a-ulid.sst"),
        Path::from(format!("{}.bak", owned[0])),
        Path::from("retired/manifest/00000000000000000001.manifest"),
    ]);
    for path in owned.iter().chain(&foreign) {
        store
            .put(path, Bytes::from_static(b"data").into())
            .await
            .unwrap();
    }
    let admin = AdminBuilder::new("retired", store.clone()).build();
    let mut after = None;
    for (examined, deleted, next) in [
        (2, 1, Some(owned[1].clone())),
        (2, 1, Some(owned[3].clone())),
        (1, 1, None),
    ] {
        let result = admin
            .collect_retired_ssts_after_quiescence(RetiredSstGcOptions {
                protected: &protected,
                after: after.as_ref(),
                max_objects: 2,
                is_authorized: &|| true,
            })
            .await
            .unwrap();
        assert_eq!(
            (result.examined, result.deleted, &result.next),
            (examined, deleted, &next)
        );
        after = result.next;
    }
    assert_eq!(
        paths(store.as_ref()).await,
        foreign.union(&protected).cloned().collect()
    );
}

#[tokio::test]
async fn retired_gc_rejects_invalid_budget_and_cursor_without_deleting() {
    let store = Arc::new(InMemory::new());
    let owned = sst_path("retired/compacted", 1);
    store
        .put(&owned, Bytes::from_static(b"data").into())
        .await
        .unwrap();
    let admin = AdminBuilder::new("retired", store.clone()).build();
    let protected = BTreeSet::new();
    let invalid_cursors = [
        sst_path("other/compacted", 1),
        sst_path("retired/compacted/nested", 1),
        Path::from("retired/compacted/not-a-ulid.sst"),
        Path::from("retired/compacted"),
    ];
    for (max_objects, after) in [0, 1025, usize::MAX]
        .into_iter()
        .map(|budget| (budget, None))
        .chain(invalid_cursors.iter().map(|cursor| (1, Some(cursor))))
    {
        assert!(admin
            .collect_retired_ssts_after_quiescence(RetiredSstGcOptions {
                protected: &protected,
                after,
                max_objects,
                is_authorized: &|| true,
            })
            .await
            .is_err());
        assert_eq!(paths(store.as_ref()).await, BTreeSet::from([owned.clone()]));
    }
}

#[tokio::test]
async fn retired_gc_stops_when_authorization_is_revoked_without_deleting() {
    let store = Arc::new(InMemory::new());
    let owned = sst_path("retired/compacted", 1);
    store
        .put(&owned, Bytes::from_static(b"data").into())
        .await
        .unwrap();
    let admin = AdminBuilder::new("retired", store.clone()).build();
    let protected = BTreeSet::new();
    for initially_authorized in [false, true] {
        let authorized = AtomicBool::new(initially_authorized);
        assert!(admin
            .collect_retired_ssts_after_quiescence(RetiredSstGcOptions {
                protected: &protected,
                after: None,
                max_objects: 1,
                is_authorized: &|| authorized.swap(false, Ordering::SeqCst),
            })
            .await
            .is_err());
        assert_eq!(paths(store.as_ref()).await, BTreeSet::from([owned.clone()]));
    }
}

#[tokio::test]
async fn manifest_fence_advances_version_and_preserves_state() {
    let store = Arc::new(InMemory::new());
    let db = Db::builder("retired", store.clone())
        .with_settings(Settings {
            compactor_options: None,
            garbage_collector_options: None,
            ..Settings::default()
        })
        .build()
        .await
        .unwrap();
    db.put(b"key", b"value").await.unwrap();
    db.close().await.unwrap();
    let admin = AdminBuilder::new("retired", store).build();
    admin
        .create_detached_checkpoint(&CheckpointOptions::default())
        .await
        .unwrap();
    let before = admin.read_manifest(None).await.unwrap().unwrap();
    assert!(!before.l0().is_empty());
    assert!(!before.checkpoints().is_empty());

    let version = admin
        .fence_manifest_after_quiescence()
        .await
        .unwrap()
        .unwrap();
    let after = admin.read_manifest(None).await.unwrap().unwrap();
    assert_eq!(version, before.id() + 1);
    assert_eq!(after.id(), version);
    let mut before_state = serde_json::to_value(&before).unwrap();
    let mut after_state = serde_json::to_value(&after).unwrap();
    before_state.as_object_mut().unwrap().remove("id");
    after_state.as_object_mut().unwrap().remove("id");
    assert_eq!(before_state, after_state);
    assert_eq!(
        admin.read_manifest(Some(before.id())).await.unwrap(),
        Some(before)
    );
}

#[tokio::test]
async fn manifest_fence_does_not_create_an_absent_database() {
    let store = Arc::new(InMemory::new());
    let admin = AdminBuilder::new("retired", store.clone()).build();
    assert_eq!(admin.fence_manifest_after_quiescence().await.unwrap(), None);
    assert!(paths(store.as_ref()).await.is_empty());
}

#[derive(Debug, Default)]
struct FaultStore {
    inner: InMemory,
    revoke_on_list: Option<Arc<AtomicBool>>,
    listed: Arc<AtomicUsize>,
    fail_compaction_put: bool,
    failed_compaction_put: AtomicBool,
}

impl FaultStore {
    fn track_listing(
        &self,
        listing: BoxStream<'static, object_store::Result<ObjectMeta>>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let listed = self.listed.clone();
        let authorized = self.revoke_on_list.clone();
        listing
            .inspect(move |_| {
                listed.fetch_add(1, Ordering::SeqCst);
                if let Some(authorized) = &authorized {
                    authorized.store(false, Ordering::SeqCst);
                }
            })
            .boxed()
    }
}

impl std::fmt::Display for FaultStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("FaultStore")
    }
}

#[async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        if self.fail_compaction_put && path.as_ref().contains("/compactions/") {
            self.failed_compaction_put.store(true, Ordering::SeqCst);
            return Err(object_store::Error::NotFound {
                path: path.to_string(),
                source: "injected compactor setup failure".into(),
            });
        }
        self.inner.put_opts(path, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }

    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.inner.get_opts(path, options).await
    }

    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(paths)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.track_listing(self.inner.list(prefix))
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.track_listing(self.inner.list_with_offset(prefix, offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[tokio::test]
async fn retired_gc_stops_listing_when_authorization_is_revoked() {
    let authorized = Arc::new(AtomicBool::new(true));
    let store = Arc::new(FaultStore {
        revoke_on_list: Some(authorized.clone()),
        ..FaultStore::default()
    });
    for id in 1..=16 {
        store
            .put(
                &sst_path("retired/compacted", id),
                Bytes::from_static(b"data").into(),
            )
            .await
            .unwrap();
    }
    let admin = AdminBuilder::new("retired", store.clone()).build();
    for after in [None, Some(sst_path("retired/compacted", 1))] {
        authorized.store(true, Ordering::SeqCst);
        store.listed.store(0, Ordering::SeqCst);
        assert!(admin
            .collect_retired_ssts_after_quiescence(RetiredSstGcOptions {
                protected: &BTreeSet::new(),
                after: after.as_ref(),
                max_objects: 1,
                is_authorized: &|| authorized.load(Ordering::SeqCst),
            })
            .await
            .is_err());
        assert_eq!(store.listed.load(Ordering::SeqCst), 1);
        assert_eq!(paths(&store.inner).await.len(), 16);
    }
}

#[tokio::test]
async fn failed_compactor_setup_releases_unstarted_writer_and_backend() {
    let store = Arc::new(FaultStore {
        fail_compaction_put: true,
        ..FaultStore::default()
    });
    let weak = Arc::downgrade(&store);
    let result = Db::builder("retired", store.clone())
        .with_settings(Settings {
            flush_interval: None,
            object_store_max_retries: Some(0),
            ..Settings::default()
        })
        .build()
        .await;
    assert!(result.is_err());
    assert!(store.failed_compaction_put.load(Ordering::SeqCst));
    drop(result);
    drop(store);
    assert!(weak.upgrade().is_none());
}
