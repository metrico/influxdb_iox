//! Convert TSM into RecordBatches (and IOx schema)

pub struct TsmConverter {
}

// Basic flow will be:
// find each measurement
// figure out schema
// bash out record batches
