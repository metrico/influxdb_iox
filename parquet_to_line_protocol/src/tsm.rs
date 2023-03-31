//! Convert TSM into RecordBatches (and IOx schema)

use std::collections::{HashMap, HashSet};

use datafusion::arrow::datatypes::DataType;
use influxdb_tsm::{reader::IndexEntry, BlockType};
use schema::{SchemaBuilder, Schema};

/// all index entries for a particular measurement
#[derive(Debug)]
pub struct TsmMeasurement {
    index_entries: Vec<IndexEntry>,
    schema_builder: SchemaBuilder,
    column_names: HashSet<String>,
}

impl TsmMeasurement {
    pub fn new() -> Self {
        let mut schema_builder = SchemaBuilder::new();

        schema_builder.timestamp();

        Self {
            index_entries: vec![],
            schema_builder,
            column_names: HashSet::new()
        }
    }

    /// inserts an index entry for the specified tags / fields
    /// also building up the schema as needed
    /// tagset: (name, value)
    /// field_key: field_name
    pub fn insert(&mut self, tagset: Vec<(String, String)>, field_key: String, index: IndexEntry) {

        // add all columns we haven't seen before
        let tag_column_names = tagset.iter().map(|(tag_name, _tag_value)| tag_name);

        for tag_name in tag_column_names {
            if !self.column_names.contains(tag_name) {
                self.schema_builder.tag(&tag_name);
                self.column_names.insert(tag_name.clone());
            }
        }

        let data_type = column_type(index.block_type);

        if !self.column_names.contains(&field_key) {
            self.schema_builder.field(&field_key,data_type).unwrap();
            self.column_names.insert(field_key.clone());
        }
    }

    pub fn schema(&self) -> Schema {
        self.schema_builder.clone().build().unwrap()
    }

}

#[derive(Debug, Default)]
pub struct TsmNamespace {
    // Maps measurement name to  TsmMeasurements
    measurements: HashMap<String, TsmMeasurement>,
}

impl TsmNamespace {
    pub fn new() -> Self {
        Default::default()
    }

    /// return a TsmMeasurement for the measurement name
    pub fn get_mut(&mut self, name: &str) -> &mut TsmMeasurement {
        self.measurements.entry(name.to_string())
            .or_insert_with(|| TsmMeasurement::new())
    }

    pub fn dump(&self)  {
        for (name, measurement) in self.measurements.iter() {
            println!("Measurement Name: {name}");
            println!("Schema:");
            println!("{:#?}", measurement.schema());
        }
    }
}



#[derive(Debug)]
pub struct TsmConverter {

}

fn column_type(block_type: BlockType) -> DataType {
    match block_type {
        BlockType::Float => DataType::Float64,
        BlockType::Integer => DataType::Int64,
        BlockType::Bool => DataType::Boolean,
        BlockType::Str => DataType::Utf8,
        BlockType::Unsigned => DataType::UInt64,
    }
}

// Basic flow will be:
// find each measurement
// figure out schema
// bash out record batches
