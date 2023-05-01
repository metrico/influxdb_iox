use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// A partition template specified by a namespace record.
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct NamespacePartitionTemplateOverride(PartitionTemplate);

impl NamespacePartitionTemplateOverride {
    /// Create a new, immutable override for a namespace's partition template.
    pub fn new(partition_template: PartitionTemplate) -> Self {
        Self(partition_template)
    }
}

impl Default for NamespacePartitionTemplateOverride {
    fn default() -> Self {
        Self(PARTITION_BY_DAY.clone())
    }
}

/// A partition template specified by a table record.
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct TablePartitionTemplateOverride(PartitionTemplate);

impl TablePartitionTemplateOverride {
    /// Create a new, immutable override for a table's partition template.
    pub fn new(partition_template: PartitionTemplate) -> Self {
        Self(partition_template)
    }
}

/// This is used when setting a new table's override to the namespace's override because no table
/// override has been specified during creation.
impl From<&NamespacePartitionTemplateOverride> for TablePartitionTemplateOverride {
    fn from(namespace: &NamespacePartitionTemplateOverride) -> Self {
        Self(namespace.0.clone())
    }
}

impl Default for TablePartitionTemplateOverride {
    fn default() -> Self {
        Self(PARTITION_BY_DAY.clone())
    }
}

/// A partition template specified as the default to be used in the absence of any overrides.
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub struct DefaultPartitionTemplate(&'static PartitionTemplate);

impl Default for DefaultPartitionTemplate {
    fn default() -> Self {
        Self(&PARTITION_BY_DAY)
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

impl PartitionTemplate {
    /// If the table has a partition template, use that. Otherwise, if the namespace has a
    /// partition template, use that. If neither the table nor the namespace has a template,
    /// use the default template.
    pub fn determine_precedence<'a>(
        table: Option<&'a Arc<TablePartitionTemplateOverride>>,
        namespace: Option<&'a Arc<NamespacePartitionTemplateOverride>>,
        default: &'a DefaultPartitionTemplate,
    ) -> &'a PartitionTemplate {
        table
            .map(|t| &t.0)
            .or(namespace.map(|n| &n.0))
            .unwrap_or(default.0)
    }
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
