use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// A partition template specified by a namespace record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespacePartitionTemplateOverride(
    /// A namespace's partition template is copied to a table when that table is created in the
    /// namespace and a custom table override hasn't been specified. The namespace partition
    /// template will never be used to partition writes (only the table partition template will
    /// be), so the namespace partition template never needs to be deserialized into the
    /// [`PartitionTemplate`] type that [`mutable_batch::PartitionWrite::partition`] needs.
    Arc<sqlx::types::JsonRawValue>,
);

impl NamespacePartitionTemplateOverride {
    /// Create a new, immutable override for a namespace's partition template.
    pub fn new(partition_template: Arc<sqlx::types::JsonRawValue>) -> Self {
        Self(partition_template)
    }
}

/// A partition template specified by a table record, used when partitioning writes for that table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TablePartitionTemplateOverride(
    /// When a table is retrieved from the catalog and its data cached in a [`TableSchema`], the
    /// data will be used to partition writes, so we need to deserialize and save the
    /// [`PartitionTemplate`].
    Option<PartitionTemplate>,
);

impl TablePartitionTemplateOverride {
    /// Create a new, immutable override for a table's partition template.
    pub fn new(partition_template: Option<PartitionTemplate>) -> Self {
        Self(partition_template)
    }

    /// Read access to the inner [`PartitionTemplate`].
    pub fn inner(&self) -> &PartitionTemplate {
        self.0.as_ref().unwrap_or(&PARTITION_BY_DAY)
    }
}

/// The default partitioning scheme is by each day according to the "time" column.
pub static PARTITION_BY_DAY: Lazy<PartitionTemplate> = Lazy::new(|| PartitionTemplate {
    parts: vec![TemplatePart::TimeFormat("%Y-%m-%d".to_owned())],
});

/// `PartitionTemplate` is used to compute the partition key of each row that gets written. It can
/// consist of a column name and its value or a formatted time. For columns that do not appear in
/// the input row, a blank value is output.
///
/// The key is constructed in order of the template parts; thus ordering changes what partition key
/// is generated.
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
#[allow(missing_docs)]
pub struct PartitionTemplate {
    pub parts: Vec<TemplatePart>,
}

/// `TemplatePart` specifies what part of a row should be used to compute this
/// part of a partition key.
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub enum TemplatePart {
    /// The value in a named column
    Column(String),
    /// Applies a  `strftime` format to the "time" column.
    ///
    /// For example, a time format of "%Y-%m-%d %H:%M:%S" will produce
    /// partition key parts such as "2021-03-14 12:25:21" and
    /// "2021-04-14 12:24:21"
    TimeFormat(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_template_to_json() {
        let partition_template = PartitionTemplate {
            parts: vec![
                TemplatePart::Column("tag1".into()),
                TemplatePart::TimeFormat("%Y-%m".into()),
            ],
        };

        let json = serde_json::to_string(&partition_template).unwrap();

        assert_eq!(
            json,
            "{\"parts\":[\
                {\"Column\":\"tag1\"},\
                {\"TimeFormat\":\"%Y-%m\"}\
            ]}"
        );

        let back: PartitionTemplate = serde_json::from_str(&json).unwrap();
        assert_eq!(partition_template, back);
    }
}
