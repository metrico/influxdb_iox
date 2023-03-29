use serde::Deserialize;

use data_types::NamespaceName;

#[derive(Clone, Debug, Deserialize)]
pub(crate) enum Precision {
    #[serde(rename = "s")]
    Seconds,
    #[serde(rename = "ms")]
    Milliseconds,
    #[serde(rename = "us")]
    Microseconds,
    #[serde(rename = "ns")]
    Nanoseconds,
}

impl Default for Precision {
    fn default() -> Self {
        Self::Nanoseconds
    }
}

impl Precision {
    /// Returns the multiplier to convert to nanosecond timestamps
    pub(crate) fn timestamp_base(&self) -> i64 {
        match self {
            Precision::Seconds => 1_000_000_000,
            Precision::Milliseconds => 1_000_000,
            Precision::Microseconds => 1_000,
            Precision::Nanoseconds => 1,
        }
    }
}

#[derive(Debug)]
/// Standardized DML operation parameters
pub struct WriteInfo {
    pub(crate) namespace: NamespaceName<'static>,
    pub(crate) precision: Precision,
}
