use crate::{
    influxdata::iox::namespace::v1 as proto,
    google::{FieldViolation, FromOptionalField, FromRepeatedField, OptionalField},
};
use data_types::{PartitionTemplate, TemplatePart};

impl TryFrom<proto::PartitionTemplate> for PartitionTemplate {
    type Error = FieldViolation;

    fn try_from(value: proto::PartitionTemplate) -> Result<Self, Self::Error> {
        Ok(Self {
            parts: value.parts.repeated("parts")?,
        })
    }
}

impl TryFrom<proto::TemplatePart> for TemplatePart {
    type Error = FieldViolation;

    fn try_from(value: proto::TemplatePart) -> Result<Self, Self::Error> {
        let part = value.part.unwrap_field("part")?;

        Ok(match part {
            proto::template_part::Part::ColumnValue(value) => Self::Column(value),
            proto::template_part::Part::TimeFormat(value) => Self::TimeFormat(value),
        })
    }
}
