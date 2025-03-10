use std::sync::Once;
/**
 * The deltalake crate is currently just a meta-package shim for deltalake-core
 */
pub use deltalake_core::*;

#[cfg(any(feature = "s3", feature = "s3-native-tls"))]
pub use deltalake_aws as aws;
#[cfg(feature = "azure")]
pub use deltalake_azure as azure;
#[cfg(feature = "unity-experimental")]
pub use deltalake_catalog_unity as unity_catalog;
#[cfg(feature = "gcs")]
pub use deltalake_gcp as gcp;
#[cfg(feature = "hdfs")]
pub use deltalake_hdfs as hdfs;
#[cfg(feature = "lakefs")]
pub use deltalake_lakefs as lakefs;

pub fn ensure_initialized() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        #[cfg(any(feature = "s3", feature = "s3-native-tls"))]
        aws::register_handlers(None);
        #[cfg(feature = "azure")]
        azure::register_handlers(None);
        #[cfg(feature = "gcs")]
        gcp::register_handlers(None);
        #[cfg(feature = "hdfs")]
        hdfs::register_handlers(None);
    })
}
