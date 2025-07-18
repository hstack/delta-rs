use crate::kernel::snapshot::log_segment::LogSegment;
use crate::logstore::{LogStore, LogStoreExt};
use crate::{DeltaResult, DeltaTableError};
use delta_kernel::path::{LogPathFileType, ParsedLogPath};
use futures::TryStreamExt;
use object_store::{ObjectMeta, ObjectStore};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::num::{NonZeroU64, NonZeroUsize};
use std::ops::RangeBounds;
use strum::Display;
use tracing::{debug, info, trace, warn};

#[derive(Debug, Clone, PartialEq, Display, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OversizePolicy {
    Reject,
    /// Skip checkpoints and only load JSON commits.
    UseTruncatedCommitLog(NonZeroUsize),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogSizeLimiter {
    /// Maximum allowed size in bytes for the total log segment (checkpoint + commit files).
    size_limit: NonZeroU64,
    oversize_policy: OversizePolicy,
}


impl LogSizeLimiter {

    pub fn new(size_limit: NonZeroU64, oversize_policy: OversizePolicy) -> Self {
        Self {
            size_limit,
            oversize_policy,
        }
    }

    pub fn try_new(size_limit: u64, truncated_commit_log_size: Option<usize>) -> DeltaResult<Self> {
        let size_limit = NonZeroU64::new(size_limit)
            .ok_or_else(|| DeltaTableError::Generic("max_log_bytes must be nonzero".into()))?;
        let oversize_policy = if let Some(num_commits) = truncated_commit_log_size {
            let num_commits = NonZeroUsize::new(num_commits)
                .ok_or_else(|| DeltaTableError::Generic("pseudo_cdf_lookback_count must be nonzero".into()))?;
            OversizePolicy::UseTruncatedCommitLog(num_commits)
        } else {
            OversizePolicy::Reject
        };
        Ok(Self {
            size_limit,
            oversize_policy,
        })
    }

    pub fn from_storage_options(opts: &mut HashMap<String, String>) -> DeltaResult<Option<Self>> {
        let prefix = "log_size_limiter";
        let size_limit_key = &format!("{prefix}.size_limit");
        let use_commit_log_key = &format!("{prefix}.use_truncated_commit_log");
        let num_commits_key = &format!("{prefix}.truncated_commit_log_size");

        let size_limit: Option<u64> = opts.remove(size_limit_key)
            .map(|opt| opt.parse()
                .expect(&format!("{size_limit_key} must be a positive int; got {opt}")));
        let use_commit_log: bool = opts.remove(use_commit_log_key)
            .map(|opt| opt.parse()
                .expect(&format!("{use_commit_log_key} must be a boolean; got {opt}")))
            .unwrap_or(false);
        let num_commits: usize = opts.remove(num_commits_key)
            .map(|opt| opt.parse()
                .expect(&format!("{num_commits_key} must be a positive int; got {opt}")))
            .unwrap_or(24); // default number of commits to use when commit log is enabled with no size specified
        size_limit
            .map(|limit| LogSizeLimiter::try_new(limit, use_commit_log.then_some(num_commits)))
            .transpose()
    }

    pub(super) async fn truncate(&self, log_segment: LogSegment, log_store: &dyn LogStore) -> DeltaResult<LogSegment> {
        let total_size: u64 = log_segment
            .checkpoint_files
            .iter()
            .chain(log_segment.commit_files.iter())
            .map(|f| f.size)
            .sum();
        let total_size = total_size;
        let size_limit = self.size_limit.get();

        if total_size > size_limit {
            warn!(
                "Log segment size in bytes: {} > {}. Applying policy: {:?}",
                total_size, size_limit, self.oversize_policy
            );
            trace!("Oversized log segment: {:?}", log_segment);
            match &self.oversize_policy {
                OversizePolicy::Reject =>
                    Err(DeltaTableError::Generic(format!(r#"
                        Table log segment size ({} bytes) exceeds maximum allowed size ({} bytes).
                        Consider increasing the size limit or using an oversize policy other than {}.
                    "#, total_size, self.size_limit, self.oversize_policy))),
                OversizePolicy::UseTruncatedCommitLog(num_commits) =>
                    truncated_commit_log(log_segment, log_store, num_commits, size_limit).await,
            }
        } else {
            debug!("Log segment size ({} bytes) is within the limit of {} bytes", total_size, size_limit);
            Ok(log_segment)
        }
    }
}

async fn truncated_commit_log(log_segment: LogSegment, log_store: &dyn LogStore, num_commits: &NonZeroUsize, size_limit: u64) -> DeltaResult<LogSegment> {
    let num_commits = num_commits.get();
    let truncated_log: Vec<ObjectMeta> = if log_segment.commit_files.len() < num_commits {
        let segment_version = log_segment.version as usize;
        let first_missing_version = segment_version.saturating_sub(num_commits - 1); // start from zero if num_commits > segment_version
        let last_missing_version = segment_version - log_segment.commit_files.len(); // cannot overflow
        info!("Extending the segment commit log with versions {}-{}", first_missing_version, last_missing_version);
        let missing_versions = first_missing_version..=last_missing_version;
        let additional_commits = list_commit_files(log_store, missing_versions).await?;
        log_segment.commit_files.into_iter()
            .chain(additional_commits)
            .collect()
    } else {
        info!("Discarding the last {} entries from the segment commit log", log_segment.commit_files.len() - num_commits);
        log_segment.commit_files.into_iter()
            .take(num_commits)
            .collect()
    };
    let mut truncated_log_size = 0_u64; // keep track of the total size to cut it shorter if needed
    Ok(LogSegment {
        version: log_segment.version,
        commit_files: truncated_log.into_iter().take_while(|f| {
            truncated_log_size += f.size;
            truncated_log_size <= size_limit
        }).collect(),
        checkpoint_files: vec![],
    })
}

async fn list_commit_files(
    log_store: &dyn LogStore,
    version_range: impl RangeBounds<usize>,
) -> DeltaResult<Vec<ObjectMeta>> {
    let root_store = log_store.root_object_store(None);
    log_store.refresh().await?;
    let log_url = log_store.log_root_url();
    let mut store_root = log_url.clone();
    store_root.set_path("");
    let log_path = crate::logstore::object_store_path(&log_url)?;
    let mut commit_files = root_store
        .list(Some(&log_path))
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .filter_map(|obj_meta|
            store_root.join(obj_meta.location.as_ref()).ok()
                .and_then(|url| ParsedLogPath::try_from(url).ok()?)
                .map(|parsed| (obj_meta, parsed))
        )
        .filter(|meta| matches!(meta.1.file_type, LogPathFileType::Commit))
        .filter(|meta| version_range.contains(&(meta.1.version as usize)))
        .map(|(meta, _path)| meta)
        .collect::<Vec<_>>();
    commit_files.sort_unstable_by(|left, right| right.location.cmp(&left.location));
    Ok(commit_files)
}


#[cfg(test)]
mod tests {
    use super::*;
    use test_doubles::*;
    use crate::{DeltaResult, DeltaTableBuilder};
    use std::collections::HashMap;
    use std::num::NonZeroUsize;

    #[test]
    fn test_serde() -> DeltaResult<()> {
        let json = r#"{
            "size_limit": 10055,
            "oversize_policy": "reject"
        }"#;
        assert_eq!(
            serde_json::from_str::<LogSizeLimiter>(json)?,
            LogSizeLimiter::new(
                NonZeroU64::new(10055).unwrap(),
                OversizePolicy::Reject,
            )
        );

        let json = r#"{
            "size_limit": 10055,
            "oversize_policy": {
                "use_truncated_commit_log": 100
            }
        }"#;
        assert_eq!(
            serde_json::from_str::<LogSizeLimiter>(json)?,
            LogSizeLimiter::new(
                NonZeroU64::new(10055).unwrap(),
                OversizePolicy::UseTruncatedCommitLog(NonZeroUsize::new(100).unwrap()),
            )
        );
        Ok(())
    }

    #[test]
    fn test_from_storage_opts() -> DeltaResult<()> {
        assert_eq!(
            LogSizeLimiter::from_storage_options(&mut HashMap::new())?,
            None
        );
        let mut opts = HashMap::from([
            ("log_size_limiter.size_limit".into(), "10".into()),
            ("log_size_limiter.use_truncated_commit_log".into(), "false".into()),
            ("log_size_limiter.truncated_commit_log_size".into(), "5".into()), // should be ignored
            ("test".into(), "1".into()),
        ]);
        assert_eq!(
            LogSizeLimiter::from_storage_options(&mut opts)?,
            Some(LogSizeLimiter::new(
                NonZeroU64::new(10).unwrap(),
                OversizePolicy::Reject
            ))
        );
        assert_eq!(opts.len(), 1);
        assert!(opts.contains_key("test"));
        Ok(())
    }

    #[test]
    fn test_storage_opts_propagation() -> DeltaResult<()> {
        let table = DeltaTableBuilder::from_uri("memory:///")
            .with_storage_options(HashMap::from([
                ("log_size_limiter.size_limit".into(), "10".into()),
                ("log_size_limiter.use_truncated_commit_log".into(), "true".into()),
                ("log_size_limiter.truncated_commit_log_size".into(), "5".into()),
            ])).build()?;
        assert_eq!(
            table.config.log_size_limiter.expect("LogSizeLimiter should be set"),
            LogSizeLimiter::new(
                NonZeroU64::new(10).unwrap(),
                OversizePolicy::UseTruncatedCommitLog(NonZeroUsize::new(5).unwrap())
            )
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_noop_within_limits() -> DeltaResult<()> {
        let fixture = TestFixture::default();
        let limiter = LogSizeLimiter::new(
            NonZeroU64::new(5000).unwrap(),
            OversizePolicy::Reject,
        );
        let segment = fixture.segment_with_checkpoint(0, 2);
        assert_eq!(
            limiter.truncate(segment.clone(), fixture.log_store()).await?,
            segment
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_reject_policy() -> DeltaResult<()> {
        let fixture = TestFixture::default();
        let limiter = LogSizeLimiter::new(
            NonZeroU64::new(321).unwrap(),
            OversizePolicy::Reject,
        );
        let segment = fixture.segment_with_checkpoint(0, 2);
        let result = limiter.truncate(segment, fixture.log_store()).await;

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("exceeds maximum allowed size"));
        assert!(error_msg.contains(&format!("{} bytes", fixture.file_sizes.checkpoint + fixture.file_sizes.commit * 2)));
        assert!(error_msg.contains("321 bytes"));
        Ok(())
    }

    #[test]
    fn test_log_segment_generator() -> DeltaResult<()> {
        // meta: testing the test code
        let fixture = TestFixture::default();
        assert_eq!(
            fixture.segment_with_checkpoint(1, 2), // checkpoint at version 1 and 2 subsequent commits
            LogSegment {
                version: 3,
                commit_files: vec![
                    obj_meta("_delta_log/00000000000000000003.json", 128),
                    obj_meta("_delta_log/00000000000000000002.json", 128),
                ].into(),
                checkpoint_files: vec![
                    obj_meta("_delta_log/00000000000000000001.checkpoint.parquet", 1024),
                ],
            },
        );
        assert_eq!(
            fixture.segment_with_commits_only(5..=8),
            LogSegment {
                version: 8,
                commit_files: vec![
                    obj_meta("_delta_log/00000000000000000008.json", 128),
                    obj_meta("_delta_log/00000000000000000007.json", 128),
                    obj_meta("_delta_log/00000000000000000006.json", 128),
                    obj_meta("_delta_log/00000000000000000005.json", 128),
                ].into(),
                checkpoint_files: vec![],
            },
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_fake_store() -> DeltaResult<()> {
        let fixture = TestFixture::new(
            FileSizes::default(),
            PersistedLog {
                commit_range: 2..=105, // the first two commits are missing from the store
                checkpoint_cadence: 10, // checkpoints at 10, 20, 30, ...
            },
        );
        assert_eq!(
            LogSegment::try_new(fixture.log_store(), Some(12)).await?,
            fixture.segment_with_checkpoint(10, 2),
        );
        assert_eq!(
            LogSegment::try_new(fixture.log_store(), None).await?, // latest version
            fixture.segment_with_checkpoint(100, 5),
        );
        assert_eq!(
            LogSegment::try_new(fixture.log_store(), Some(8)).await?,
            fixture.segment_with_commits_only(2..=8), // no checkpoint before version 10 and stored commits start at 2
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_commit_log_truncation() -> DeltaResult<()> {
        let fixture = TestFixture::new(
            FileSizes {
                commit: 10,
                checkpoint: 1000,
            },
            PersistedLog {
                commit_range: 0..=100,
                checkpoint_cadence: 5, // persisted checkpoints are always ignored by this policy, so they're just noise here
            },
        );
        let limiter = LogSizeLimiter::new(
            NonZeroU64::new(500).unwrap(), // smaller than the checkpoint size, can fit 50 commits
            OversizePolicy::UseTruncatedCommitLog(NonZeroUsize::new(10).unwrap()),
        );

        let segment = fixture.segment_with_checkpoint(25, 0);
        // checkpoint at version 25 and no subsequent commits; should load 10 commits from the store
        assert_eq!(
            limiter.truncate(segment, fixture.log_store()).await?,
            fixture.segment_with_commits_only(16..=25),
        );

        let segment = fixture.segment_with_checkpoint(1, 2);
        // assume checkpoint at version 1 and 2 subsequent commits; should load commits 0-3 from the store
        assert_eq!(
            limiter.truncate(segment, fixture.log_store()).await?,
            fixture.segment_with_commits_only(0..=3),
        );

        let segment = fixture.segment_with_checkpoint(15, 5);
        assert_eq!(
            limiter.truncate(segment, fixture.log_store()).await?,
            fixture.segment_with_commits_only(11..=20),
        );

        let segment = fixture.segment_with_commits_only(21..=50);
        // size limit NOT exceeded: 10 bytes * 30 commits < 500; should keep the segment as is
        assert_eq!(
            limiter.truncate(segment.clone(), fixture.log_store()).await?,
            segment,
        );

        let segment = fixture.segment_with_commits_only(0..=99);
        // log size exceeded: 10 bytes * 100 commits > 500; should truncate to the last 10 commits
        assert_eq!(
            limiter.truncate(segment, fixture.log_store()).await?,
            fixture.segment_with_commits_only(90..=99),
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_commit_log_truncation_with_vacuumed_store() -> DeltaResult<()> {
        let fixture = TestFixture::new(
            FileSizes {
                commit: 10,
                checkpoint: 1000,
            },
            PersistedLog {
                commit_range: 30..=150, // commits 0-29 have been vacuumed
                checkpoint_cadence: 25, // checkpoints generated as noise for versions: 50, 75, ...
            },
        );
        let limiter = LogSizeLimiter::new(
            NonZeroU64::new(500).unwrap(), // smaller than the checkpoint size, can fit 50 commits
            OversizePolicy::UseTruncatedCommitLog(NonZeroUsize::new(100).unwrap()), // go back 100 commits
        );
        let segment = fixture.segment_with_checkpoint(32, 5);
        assert_eq!(
            limiter.truncate(segment, fixture.log_store()).await?,
            fixture.segment_with_commits_only(30..=37),
        );
        let segment = fixture.segment_with_checkpoint(144, 1);
        // only loads 50 commits instead of the configured 100 to stay within the size limit
        assert_eq!(
            limiter.truncate(segment, fixture.log_store()).await?,
            fixture.segment_with_commits_only(96..=145),
        );
        Ok(())
    }

    mod test_doubles {
        use crate::kernel::log_segment::LogSegment;
        use crate::logstore::{LogStore, LogStoreConfig};
        use crate::DeltaResult;
        use async_trait::async_trait;
        use bytes::Bytes;
        use futures::stream;
        use futures::stream::BoxStream;
        use object_store::path::Path;
        use object_store::{GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult};
        use std::ops::{Range, RangeInclusive};
        use std::sync::Arc;
        use rand::seq::SliceRandom;
        use rand::thread_rng;
        use url::Url;
        use uuid::Uuid;

        #[derive(Debug, Clone, Copy)]
        pub(super) struct FileSizes {
            pub(super) commit: u64,
            pub(super) checkpoint: u64
        }

        impl Default for FileSizes {
            fn default() -> Self {
                Self {
                    commit: 128,
                    checkpoint: 1024,
                }
            }
        }

        #[derive(Debug, Clone)]
        pub(super) struct PersistedLog {
            pub(super) commit_range: RangeInclusive<usize>,
            pub(super) checkpoint_cadence: usize,
        }

        impl Default for PersistedLog {
            fn default() -> Self {
                Self {
                    commit_range: 0..=100,
                    checkpoint_cadence: 10,
                }
            }
        }

        #[derive(Debug, Clone)]
        pub(super) struct TestFixture {
            log_store: TestLogStore,
            pub(super) file_sizes: FileSizes,
        }

        impl TestFixture {
            pub(super) fn new(
                file_sizes: FileSizes,
                persisted_log: PersistedLog,
            ) -> Self {
                let commit_files = persisted_log.commit_range.clone()
                    .map(|version| commit_file(version, file_sizes.commit));
                let checkpoint_files = (0..*persisted_log.commit_range.end())
                    .skip(persisted_log.checkpoint_cadence)
                    .step_by(persisted_log.checkpoint_cadence)
                    .filter(|version| persisted_log.commit_range.contains(version))
                    .map(|version| checkpoint_file(version, file_sizes.checkpoint));
                let mut files = commit_files.chain(checkpoint_files).collect::<Vec<_>>();

                files.shuffle(&mut thread_rng()); // no order guarantees for store listing

                let log_store = TestLogStore {
                    files,
                    config: LogStoreConfig {
                        location: Url::parse("memory://test").unwrap(),
                        options: Default::default(),
                    },
                };
                Self {
                    log_store,
                    file_sizes,
                }
            }

            pub(super) fn segment_with_checkpoint(&self, checkpoint_version: usize, num_subsequent_commits: usize) -> LogSegment {
                LogSegment {
                    version: (checkpoint_version + num_subsequent_commits) as i64,
                    commit_files: (checkpoint_version + 1 ..= checkpoint_version + num_subsequent_commits)
                        .rev()
                        .map(|version| commit_file(version, self.file_sizes.commit))
                        .collect(),
                    checkpoint_files: vec![checkpoint_file(checkpoint_version, self.file_sizes.checkpoint)],
                }
            }

            pub(super) fn segment_with_commits_only(&self, version_range: RangeInclusive<usize>) -> LogSegment {
                LogSegment {
                    version: *version_range.end() as i64,
                    commit_files: version_range
                        .rev()
                        .map(|version| commit_file(version, self.file_sizes.commit))
                        .collect(),
                    checkpoint_files: vec![],
                }
            }
            pub(super) fn log_store(&self) -> &dyn LogStore {
                &self.log_store
            }
        }

        impl Default for TestFixture {
            fn default() -> Self {
                Self::new(FileSizes::default(), PersistedLog::default())
            }
        }

        #[derive(Debug, Clone)]
        struct TestLogStore {
            files: Vec<ObjectMeta>,
            config: LogStoreConfig,
        }

        #[async_trait]
        impl LogStore for TestLogStore {
            fn name(&self) -> String {
                "TestLogStore".to_string()
            }

            async fn read_commit_entry(&self, _version: i64) -> DeltaResult<Option<Bytes>> {
                unimplemented!("TestLogStore::read_commit_entry not implemented for tests")
            }

            async fn write_commit_entry(
                &self,
                _version: i64,
                _commit_or_bytes: crate::logstore::CommitOrBytes,
                _operation_id: Uuid,
            ) -> Result<(), crate::kernel::transaction::TransactionError> {
                unimplemented!("TestLogStore::write_commit_entry not implemented for tests")
            }

            async fn abort_commit_entry(
                &self,
                _version: i64,
                _commit_or_bytes: crate::logstore::CommitOrBytes,
                _operation_id: Uuid,
            ) -> Result<(), crate::kernel::transaction::TransactionError> {
                unimplemented!("TestLogStore::abort_commit_entry not implemented for tests")
            }

            async fn get_latest_version(&self, _start_version: i64) -> DeltaResult<i64> {
                unimplemented!("TestLogStore::get_latest_version not implemented for tests")
            }

            async fn peek_next_commit(
                &self,
                _current_version: i64,
            ) -> DeltaResult<crate::logstore::PeekCommit> {
                unimplemented!("TestLogStore::peek_next_commit not implemented for tests")
            }

            fn object_store(&self, _operation_id: Option<Uuid>) -> Arc<dyn ObjectStore> {
                Arc::new(self.clone())
            }

            fn root_object_store(&self, _operation_id: Option<Uuid>) -> Arc<dyn ObjectStore> {
                Arc::new(self.clone())
            }

            fn config(&self) -> &LogStoreConfig {
                &self.config
            }
        }

        impl std::fmt::Display for TestLogStore {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "TestLogStore")
            }
        }

        #[async_trait]
        impl ObjectStore for TestLogStore {
            async fn put(&self, _location: &Path, _bytes: PutPayload) -> ObjectStoreResult<PutResult> {
                unimplemented!("TestLogStore::put not implemented for tests")
            }

            async fn put_opts(
                &self,
                _location: &Path,
                _bytes: PutPayload,
                _options: PutOptions,
            ) -> ObjectStoreResult<PutResult> {
                unimplemented!("TestLogStore::put_opts not implemented for tests")
            }

            async fn put_multipart(&self, _location: &Path) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
                unimplemented!("TestLogStore::put_multipart not implemented for tests")
            }

            async fn put_multipart_opts(
                &self,
                _location: &Path,
                _opts: PutMultipartOpts,
            ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
                unimplemented!("TestLogStore::put_multipart_opts not implemented for tests")
            }

            async fn get(&self, location: &Path) -> ObjectStoreResult<GetResult> {
                let file = self.files.iter().find(|f| f.location == *location)
                    .ok_or_else(|| object_store::Error::NotFound {
                        path: location.to_string(),
                        source: Box::new(std::io::Error::new(std::io::ErrorKind::NotFound, "Not found")),
                    })?;
                Ok(GetResult {
                    payload: GetResultPayload::Stream(Box::pin(futures::stream::once(async { Ok(Bytes::new()) }))),
                    meta: file.clone(),
                    range: 0..file.size,
                    attributes: Default::default(),
                })
            }

            async fn get_opts(&self, _location: &Path, _options: GetOptions) -> ObjectStoreResult<GetResult> {
                unimplemented!("TestLogStore::get_opts not implemented for tests")
            }

            async fn get_range(&self, _location: &Path, _range: Range<u64>) -> ObjectStoreResult<Bytes> {
                unimplemented!("TestLogStore::get_range not implemented for tests")
            }

            async fn head(&self, _location: &Path) -> ObjectStoreResult<ObjectMeta> {
                unimplemented!("TestLogStore::head not implemented for tests")
            }

            async fn delete(&self, _location: &Path) -> ObjectStoreResult<()> {
                unimplemented!("TestLogStore::delete not implemented for tests")
            }

            fn list(&self, _prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
                let files = self.files.clone();
                Box::pin(stream::iter(files.into_iter().map(Ok)))
            }

            fn list_with_offset(
                &self,
                prefix: Option<&Path>,
                _offset: &Path,
            ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
                self.list(prefix)
            }

            async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
                unimplemented!("TestLogStore::list_with_delimiter not implemented for tests")
            }

            async fn copy(&self, _from: &Path, _to: &Path) -> ObjectStoreResult<()> {
                unimplemented!("TestLogStore::copy not implemented for tests")
            }

            async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> ObjectStoreResult<()> {
                unimplemented!("TestLogStore::copy_if_not_exists not implemented for tests")
            }

            async fn rename_if_not_exists(&self, _from: &Path, _to: &Path) -> ObjectStoreResult<()> {
                unimplemented!("TestLogStore::rename_if_not_exists not implemented for tests")
            }
        }

        pub(super) fn obj_meta(path: &str, size: u64) -> ObjectMeta {
            ObjectMeta {
                location: Path::from(path),
                size,
                last_modified: "2025-07-18T15:30:00Z".parse().unwrap(),
                e_tag: None,
                version: None,
            }
        }

        fn commit_file(version: usize, file_size: u64) -> ObjectMeta {
            obj_meta(&format!("_delta_log/{:020}.json", version), file_size)
        }

        fn checkpoint_file(version: usize, file_size: u64) -> ObjectMeta {
            obj_meta(&format!("_delta_log/{:020}.checkpoint.parquet", version), file_size)
        }

    }
}
