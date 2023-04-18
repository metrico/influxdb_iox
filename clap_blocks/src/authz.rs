//! CLI config for request authorization.

pub(crate) const CONFIG_AUTHZ_ENV_NAME: &str = "INFLUXDB_IOX_AUTHZ_ADDR";
pub(crate) const CONFIG_AUTHZ_FLAG_NAME: &str = "authz-addr";

/// Configuration for optional request authorization.
#[derive(Clone, Debug, Default, clap::Parser)]
pub struct AuthzConfig {
    /// Addr for connection to authz
    #[clap(long = "authz-addr", env = "INFLUXDB_IOX_AUTHZ_ADDR")]
    pub authz_addr: Option<String>,
}
