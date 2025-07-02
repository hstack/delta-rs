use crate::kernel::snapshot::log_segment::LogSegment;
use crate::logstore::{object_store_path, LogStore, LogStoreExt};
use crate::{DeltaResult, DeltaTableError};
use futures::TryStreamExt;
use object_store::{ObjectMeta, ObjectStore};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::num::{NonZeroU64, NonZeroUsize};
use std::ops::RangeInclusive;
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
            .map(|obj_meta| obj_meta.size)
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
        commit_files: truncated_log.into_iter().take_while(|obj_meta| {
            truncated_log_size += obj_meta.size;
            truncated_log_size <= size_limit
        }).collect(),
        checkpoint_files: vec![],
    })
}

async fn list_commit_files(
    log_store: &dyn LogStore,
    version_range: RangeInclusive<usize>,
) -> DeltaResult<Vec<ObjectMeta>> {
    let log_path = object_store_path(&log_store.log_root_url())?;
    let lower_bound = log_path.child(format!("{:020}", version_range.start()));
    let upper_bound = log_path.child(format!("{:020}", version_range.end() + 1));
    let mut commit_files = log_store.root_object_store(None)
        .list_with_offset(Some(&log_path), &lower_bound)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .filter(|obj_meta| obj_meta.location.extension() == Some("json"))
        .filter(|obj_meta| obj_meta.location < upper_bound)
        .collect::<Vec<_>>();
    // reverse sort by version using the file names, as per the Delta Lake specification
    commit_files.sort_unstable_by(|left, right| right.location.cmp(&left.location));
    Ok(commit_files)
}


#[cfg(test)]
mod tests {
    use super::*;
    use test_doubles::*;
    use crate::DeltaTableBuilder;

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
        let log_store = TestLogStore::new(
            CommitRange(0..=100), CheckpointCadence(10), CommitFsize(100), CheckpointFsize(3000)
        );
        let limiter = LogSizeLimiter::new(
            NonZeroU64::new(5000).unwrap(),
            OversizePolicy::Reject,
        );
        let segment = LogSegment::try_new(&log_store, None).await?;
        assert_segment_with_checkpoint(&segment, 90, 10);
        // total size < size limit
        assert_eq!(limiter.truncate(segment.clone(), &log_store).await?, segment);

        Ok(())
    }

    #[tokio::test]
    async fn test_reject_policy() -> DeltaResult<()> {
        let log_store = TestLogStore::new(
            CommitRange(0..=100), CheckpointCadence(10), CommitFsize(100), CheckpointFsize(3000)
        );
        let limiter = LogSizeLimiter::new(
            NonZeroU64::new(2500).unwrap(),
            OversizePolicy::Reject,
        );
        let segment = LogSegment::try_new(&log_store, None).await?;
        assert_segment_with_checkpoint(&segment, 90, 10);
        let result = limiter.truncate(segment, &log_store).await;

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("exceeds maximum allowed size"));
        assert!(error_msg.contains("4000 bytes"), "`{}` does not contain '4000 bytes'", error_msg);
        assert!(error_msg.contains("2500 bytes"), "`{}` does not contain '2500 bytes'", error_msg);

        Ok(())
    }

    #[tokio::test]
    async fn test_commit_log_truncation_with_regular_delta_log() -> DeltaResult<()> {
        let log_store = TestLogStore::new(
            CommitRange(0..=100), CheckpointCadence(5), CommitFsize(10), CheckpointFsize(1000)
        );
        let limiter = LogSizeLimiter::new(
            NonZeroU64::new(500).unwrap(), // smaller than the checkpoint size, can fit 50 commits
            OversizePolicy::UseTruncatedCommitLog(NonZeroUsize::new(10).unwrap()),
        );

        let segment = LogSegment::try_new(&log_store, Some(25)).await?;
        assert_segment_with_checkpoint(&segment, 25, 0);
        assert_segment_with_commits_only(&limiter.truncate(segment, &log_store).await?, 16..=25);

        let segment = LogSegment::try_new(&log_store, Some(7)).await?;
        assert_segment_with_checkpoint(&segment, 5, 2);
        assert_segment_with_commits_only(&limiter.truncate(segment, &log_store).await?, 0..=7);

        let segment = LogSegment::try_new(&log_store, Some(19)).await?;
        assert_segment_with_checkpoint(&segment, 15, 4);
        assert_segment_with_commits_only(&limiter.truncate(segment, &log_store).await?, 10..=19);

        Ok(())
    }

    #[tokio::test]
    async fn test_commit_log_truncation_with_no_checkpoints_in_log() -> DeltaResult<()> {
        let log_store = TestLogStore::new(
            CommitRange(0..=100), CheckpointCadence(200), CommitFsize(10), CheckpointFsize(1000)
        );
        let limiter = LogSizeLimiter::new(
            NonZeroU64::new(500).unwrap(), // smaller than the checkpoint size, can fit 50 commits
            OversizePolicy::UseTruncatedCommitLog(NonZeroUsize::new(10).unwrap()),
        );

        let segment = LogSegment::try_new(&log_store, Some(30)).await?;
        assert_segment_with_commits_only(&segment, 0..=30);
        // size limit not exceeded: 31 commits * 10 bytes < 500 bytes, segment not truncated
        assert_eq!(limiter.truncate(segment.clone(), &log_store).await?, segment);

        let segment = LogSegment::try_new(&log_store, Some(75)).await?;
        assert_segment_with_commits_only(&segment, 0..=75);
        // size limit exceeded: 75 commits * 10 bytes > 500 bytes; keeps the last 10 commits
        assert_segment_with_commits_only(&limiter.truncate(segment, &log_store).await?, 66..=75);

        Ok(())
    }

    #[tokio::test]
    async fn test_commit_log_truncation_with_vacuumed_log() -> DeltaResult<()> {
        let log_store = TestLogStore::new(
            CommitRange(30..=150), CheckpointCadence(25), CommitFsize(10), CheckpointFsize(1000)
        );
        let limiter = LogSizeLimiter::new(
            NonZeroU64::new(500).unwrap(), // smaller than the checkpoint size, can fit 50 commits
            OversizePolicy::UseTruncatedCommitLog(NonZeroUsize::new(50).unwrap()),
        );

        let segment = LogSegment::try_new(&log_store, Some(70)).await?;
        assert_segment_with_checkpoint(&segment, 50, 20);
        // less than 50 commits available in the vacuumed store
        assert_segment_with_commits_only(&limiter.truncate(segment, &log_store).await?, 30..=70);

        Ok(())
    }

    #[tokio::test]
    async fn test_truncated_log_gets_cut_off_to_enforce_size_limit() -> DeltaResult<()> {
        let log_store = TestLogStore::new(
            CommitRange(30..=150), CheckpointCadence(25), CommitFsize(10), CheckpointFsize(1000)
        );
        let limiter = LogSizeLimiter::new(
            NonZeroU64::new(500).unwrap(), // smaller than the checkpoint size, can fit 50 commits
            OversizePolicy::UseTruncatedCommitLog(NonZeroUsize::new(100).unwrap()), // go back 100 commits
        );

        let segment = LogSegment::try_new(&log_store, None).await?;
        assert_segment_with_checkpoint(&segment, 125, 25);
        // only loads 50 commits instead of the configured 100 to stay within the size limit
        assert_segment_with_commits_only(&limiter.truncate(segment, &log_store).await?, 101..=150);

        Ok(())
    }

    fn commit_file_name(version: usize) -> String {
        format!("{:020}.json", version)
    }

    fn checkpoint_file_name(version: usize) -> String {
        format!("{:020}.checkpoint.parquet", version)
    }

    fn extract_file_names<'a>(stored_objects: impl IntoIterator<Item=&'a ObjectMeta>) -> Vec<String> {
        stored_objects.into_iter()
            .filter_map(|obj_meta| obj_meta.location.filename().map(ToString::to_string))
            .collect()
    }

    fn assert_segment_with_checkpoint(segment: &LogSegment, checkpoint_version: usize, num_subsequent_commits: usize) {
        assert_eq!(segment.version, (checkpoint_version + num_subsequent_commits) as i64);
        assert_eq!(
            extract_file_names(&segment.checkpoint_files),
            vec![ checkpoint_file_name(checkpoint_version) ],
        );
        assert_eq!(
            extract_file_names(&segment.commit_files),
            (checkpoint_version + 1 ..= checkpoint_version + num_subsequent_commits)
                .rev()
                .map(commit_file_name)
                .collect::<Vec<_>>(),
        );
    }

    fn assert_segment_with_commits_only(log_segment: &LogSegment, versions: RangeInclusive<usize>) {
        assert_eq!(log_segment.version, *versions.end() as i64);
        assert_eq!(log_segment.checkpoint_files, vec![]);
        assert_eq!(
            extract_file_names(&log_segment.commit_files),
            versions.rev().map(commit_file_name).collect::<Vec<_>>(),
        );
    }


    mod test_doubles {
        use super::*;
        use crate::DeltaResult;
        use crate::kernel::snapshot::log_segment::LogSegment;
        use crate::kernel::transaction::TransactionError;
        use crate::logstore::{object_store_path, CommitOrBytes, LogStore, LogStoreConfig, LogStoreExt};
        use async_trait::async_trait;
        use bytes::Bytes;
        use futures::stream;
        use futures::stream::BoxStream;
        use object_store::path::Path;
        use object_store::{GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult};
        use std::ops::RangeInclusive;
        use std::sync::Arc;
        use rand::seq::SliceRandom;
        use rand::thread_rng;
        use url::Url;
        use uuid::Uuid;

        // substitute for named arguments to make the test code self documenting
        pub(super) struct CommitRange(pub(super) RangeInclusive<usize>);
        pub(super) struct CheckpointCadence(pub(super) usize);
        pub(super) struct CommitFsize(pub(super) u64);
        pub(super) struct CheckpointFsize(pub(super) u64);

        #[derive(Debug, Clone)]
        pub(super) struct TestLogStore {
            config: LogStoreConfig,
            files: Vec<ObjectMeta>,
        }

        impl TestLogStore {
            /// Commit files are generated to span the entire `CommitRange`, and checkpoints are
            /// created according to the configured `CheckpointCadence`, starting from 0 (exclusive)
            /// up to the last version in the store (also exclusive) and only for versions
            /// that are also in the `CommitRange`.
            /// E.g. commits: 15 up to 100, cadence: 10 => checkpoints at versions 20, 30, ..., 90
            pub(super) fn new(
                commit_range: CommitRange,
                checkpoint_cadence: CheckpointCadence,
                commit_fsize: CommitFsize,
                checkpoint_fsize: CheckpointFsize,
            ) -> Self {
                // get rid of the self-documenting superfluous types
                let commit_range = commit_range.0;
                let checkpoint_cadence = checkpoint_cadence.0;
                let commit_fsize = commit_fsize.0;
                let checkpoint_fsize = checkpoint_fsize.0;

                let mut store = TestLogStore {
                    config: LogStoreConfig {
                        location: Url::parse("memory://test/delta_table").unwrap(),
                        options: Default::default(),
                    },
                    files: vec![]
                };
                let path = object_store_path(&store.log_root_url()).unwrap();
                let commit_files = commit_range.clone()
                    .map(commit_file_name)
                    .map(|f| obj_meta(path.child(f), commit_fsize));
                let checkpoint_files = (0..*commit_range.end())
                    .skip(checkpoint_cadence)
                    .step_by(checkpoint_cadence)
                    .filter(|version| commit_range.contains(version))
                    .map(checkpoint_file_name)
                    .map(|f| obj_meta(path.child(f), checkpoint_fsize));
                let mut files = commit_files.chain(checkpoint_files).collect::<Vec<_>>();

                files.shuffle(&mut thread_rng()); // no order guarantees for store listing
                store.files = files;
                store
            }

        }

        #[async_trait]
        impl LogStore for TestLogStore {
            fn name(&self) -> String {
                "TestLogStore".to_string()
            }

            async fn read_commit_entry(&self, _version: i64) -> DeltaResult<Option<Bytes>> {
                unimplemented!("TestLogStore::read_commit_entry not implemented for tests")
            }

            async fn write_commit_entry(&self, _version: i64, _commit_or_bytes: CommitOrBytes, _operation_id: Uuid) -> Result<(), TransactionError> {
                unimplemented!("TestLogStore::write_commit_entry not implemented for tests")
            }

            async fn abort_commit_entry(&self, _version: i64, _commit_or_bytes: CommitOrBytes, _operation_id: Uuid) -> Result<(), TransactionError> {
                unimplemented!("TestLogStore::abort_commit_entry not implemented for tests")
            }

            async fn get_latest_version(&self, _start_version: i64) -> DeltaResult<i64> {
                unimplemented!("TestLogStore::get_latest_version not implemented for tests")
            }

            fn object_store(&self, operation_id: Option<Uuid>) -> Arc<dyn ObjectStore> {
                self.root_object_store(operation_id)
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
                write!(f, "{}", self.name())
            }
        }

        #[async_trait]
        impl ObjectStore for TestLogStore {

            async fn put_opts(&self, _location: &Path, _bytes: PutPayload, _options: PutOptions) -> ObjectStoreResult<PutResult> {
                unimplemented!("TestLogStore::put_opts not implemented for tests")
            }

            async fn put_multipart_opts(&self, _location: &Path, _opts: PutMultipartOpts) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
                unimplemented!("TestLogStore::put_multipart_opts not implemented for tests")
            }

            async fn get_opts(&self, location: &Path, _options: GetOptions) -> ObjectStoreResult<GetResult> {
                self.files.iter().find(|obj_meta| obj_meta.location == *location)
                    .map(|obj_meta| GetResult {
                        payload: GetResultPayload::Stream(Box::pin(futures::stream::once(async { Ok(Bytes::new()) }))),
                        meta: obj_meta.clone(),
                        range: 0..obj_meta.size,
                        attributes: Default::default(),
                    })
                    .ok_or_else(|| object_store::Error::NotFound {
                        path: location.to_string(),
                        source: Box::new(std::io::Error::new(std::io::ErrorKind::NotFound, "Not found")),
                    })
            }

            async fn delete(&self, _location: &Path) -> ObjectStoreResult<()> {
                unimplemented!("TestLogStore::delete not implemented for tests")
            }

            fn list(&self, _prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
                Box::pin(stream::iter(self.files.clone().into_iter().map(Ok)))
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
        }

        fn obj_meta(path: impl Into<Path>, size: u64) -> ObjectMeta {
            ObjectMeta {
                location: path.into(),
                size,
                last_modified: "2025-07-18T15:30:00Z".parse().unwrap(),
                e_tag: None,
                version: None,
            }
        }

        #[tokio::test]
        async fn test_fake_log_store() -> DeltaResult<()> {
            let log_store = TestLogStore::new(
                CommitRange(2..=97), CheckpointCadence(10), CommitFsize(128), CheckpointFsize(1024)
            );

            // before the first checkpoint
            let segment = LogSegment::try_new(&log_store, Some(5)).await?;
            assert_segment_with_commits_only(&segment, 2..=5);
            assert_eq!(
                segment,
                LogSegment {
                    version: 5,
                    commit_files: vec![ // commits 0 and 1 are missing from the log
                        obj_meta("delta_table/_delta_log/00000000000000000005.json", 128),
                        obj_meta("delta_table/_delta_log/00000000000000000004.json", 128),
                        obj_meta("delta_table/_delta_log/00000000000000000003.json", 128),
                        obj_meta("delta_table/_delta_log/00000000000000000002.json", 128),
                    ].into(),
                    checkpoint_files: vec![],
                }
            );

            // with checkpoint
            let segment = LogSegment::try_new(&log_store, Some(32)).await?;
            assert_segment_with_checkpoint(&segment, 30, 2);
            assert_eq!(
                segment,
                LogSegment {
                    version: 32,
                    commit_files: vec![
                        obj_meta("delta_table/_delta_log/00000000000000000032.json", 128),
                        obj_meta("delta_table/_delta_log/00000000000000000031.json", 128),
                    ].into(),
                    checkpoint_files: vec![
                        obj_meta("delta_table/_delta_log/00000000000000000030.checkpoint.parquet", 1024),
                    ],
                }
            );

            // latest version
            let segment = LogSegment::try_new(&log_store, None).await?;
            assert_segment_with_checkpoint(&segment, 90, 7);
            assert_eq!(
                segment,
                LogSegment {
                    version: 97,
                    commit_files: vec![
                        obj_meta("delta_table/_delta_log/00000000000000000097.json", 128),
                        obj_meta("delta_table/_delta_log/00000000000000000096.json", 128),
                        obj_meta("delta_table/_delta_log/00000000000000000095.json", 128),
                        obj_meta("delta_table/_delta_log/00000000000000000094.json", 128),
                        obj_meta("delta_table/_delta_log/00000000000000000093.json", 128),
                        obj_meta("delta_table/_delta_log/00000000000000000092.json", 128),
                        obj_meta("delta_table/_delta_log/00000000000000000091.json", 128),
                    ].into(),
                    checkpoint_files: vec![
                        obj_meta("delta_table/_delta_log/00000000000000000090.checkpoint.parquet", 1024),
                    ],
                }
            );

            Ok(())
        }
    }
}
