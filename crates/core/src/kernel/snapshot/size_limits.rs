use std::collections::HashMap;
use crate::kernel::snapshot::log_segment::LogSegment;
use crate::logstore::{LogStore, LogStoreExt};
use crate::{DeltaResult, DeltaTableError};
use delta_kernel::path::{LogPathFileType, ParsedLogPath};
use futures::TryStreamExt;
use object_store::{ObjectMeta, ObjectStore};
use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;
use std::ops::RangeBounds;
use strum::Display;
use tracing::info;

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
    size_limit: NonZeroUsize,
    oversize_policy: OversizePolicy,
}


impl LogSizeLimiter {

    pub fn new(size_limit: NonZeroUsize, oversize_policy: OversizePolicy) -> Self {
        Self {
            size_limit,
            oversize_policy,
        }
    }

    pub fn try_new(size_limit: usize, truncated_commit_log_size: Option<usize>) -> DeltaResult<Self> {
        let size_limit = NonZeroUsize::new(size_limit)
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

        let size_limit: Option<usize> = opts.remove(size_limit_key)
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

    pub(super) async fn coerce(&self, log_segment: LogSegment, log_store: &dyn LogStore) -> DeltaResult<LogSegment> {
        let total_size: u64 = log_segment
            .checkpoint_files
            .iter()
            .chain(log_segment.commit_files.iter())
            .map(|f| f.size)
            .sum();
        let total_size = total_size as usize;

        if total_size > self.size_limit.get() {
            match self.oversize_policy {
                OversizePolicy::Reject =>
                    Err(DeltaTableError::Generic(format!(r#"
                        Table log segment size ({} bytes) exceeds maximum allowed size ({} bytes).
                        Consider increasing the size limit or using an oversize policy other than {}.
                    "#, total_size, self.size_limit, self.oversize_policy))),
                OversizePolicy::UseTruncatedCommitLog(lookback_count) =>
                    truncated_commit_log(log_segment, log_store, lookback_count).await,
            }
        } else {
            Ok(log_segment)
        }
    }
}

async fn truncated_commit_log(log_segment: LogSegment, log_store: &dyn LogStore, num_commits: NonZeroUsize) -> DeltaResult<LogSegment> {
    let segment_version = log_segment.version as usize;
    let additional_commits = if log_segment.commit_files.len() < num_commits.get() {
        let start_version = segment_version.saturating_sub(num_commits.get() - 1);
        let end_version = segment_version.saturating_sub(log_segment.commit_files.len());
        list_commit_files(log_store, start_version..=end_version).await?
    } else {
        vec![]
    };
    Ok(LogSegment {
        version: log_segment.version,
        commit_files: log_segment.commit_files.into_iter()
            .chain(additional_commits.into_iter())
            .collect(),
        checkpoint_files: vec![],
    })
}

async fn list_commit_files(
    log_store: &dyn LogStore,
    version_range: impl RangeBounds<usize>,
) -> DeltaResult<Vec<ObjectMeta>> {
    info!("Listing commit files for versions {:?} - {:?}", version_range.start_bound(), version_range.end_bound());
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
        .filter_map(|f| {
            let file_url = store_root.join(f.location.as_ref()).ok()?;
            let path = ParsedLogPath::try_from(file_url).ok()??;
            Some((f, path))
        })
        .filter(|meta| matches!(meta.1.file_type, LogPathFileType::Commit))
        .filter(|meta| version_range.contains(&(meta.1.version as usize)))
        .map(|(meta, _path)| meta)
        .collect::<Vec<_>>();
    commit_files.sort_unstable_by(|left, right| right.location.cmp(&left.location));
    Ok(commit_files)
}


#[cfg(test)]
mod tests {
    use crate::kernel::snapshot::size_limits::OversizePolicy;
    use crate::kernel::snapshot::size_limits::LogSizeLimiter;
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
                NonZeroUsize::new(10055).unwrap(),
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
                NonZeroUsize::new(10055).unwrap(),
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
            ("log_size_limiter.truncated_commit_log_size".into(), "5".into()),
            ("test".into(), "1".into()),
        ]);
        assert_eq!(
            LogSizeLimiter::from_storage_options(&mut opts)?,
            Some(LogSizeLimiter::new(
                NonZeroUsize::new(10).unwrap(),
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
                NonZeroUsize::new(10).unwrap(),
                OversizePolicy::UseTruncatedCommitLog(NonZeroUsize::new(5).unwrap())
            )
        );
        Ok(())
    }
}
