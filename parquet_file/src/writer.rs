//! Memory tracked parquet writer
use std::{collections::VecDeque, fmt::Debug, io::Write, sync::Arc};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion::{
    error::DataFusionError,
    execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation},
};
use observability_deps::tracing::debug;
use parquet::{arrow::ArrowWriter, errors::ParquetError, file::properties::WriterProperties};
use thiserror::Error;

/// Default max buffer size. See [`TrackedMemoryArrowWriter::with_max_buffer_size`] for more details
pub const MAX_BUFFER_SIZE: usize = 512 * 1024 * 1024; // 512 MB

/// Default allocation increment size. See [`TrackedMemoryArrowWriter::with_min_allocation_increment`] for more details
pub const MIN_ALLOCATION_INCREMENT: usize = 64 * 1024 * 1024; // 64MB

/// Errors related to [`TrackedMemoryArrowWriter`]
#[derive(Debug, Error)]
pub enum Error {
    /// Writing the parquet file failed with the specified error.
    #[error("failed to write parquet file: {0}")]
    Writer(#[from] ParquetError),

    /// Could not allocate sufficient memory
    #[error("failed to allocate buffer while writing parquet: {0}")]
    OutOfMemory(#[from] DataFusionError),
}

/// Results!
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Wraps an [`ArrowWriter`] so that all data that is buffered is
/// accounted for in the DataFusion [`MemoryPool`]
///
/// # Behavior
///
/// Buffers [`RecordBatch`]es before writing them to the parquet
/// writer until either:
///
/// 1. No more buffer space is allowed by the `MemoryPool`
/// 2. The number of rows buffered exceeds the `max_buffer_rows` size
///
/// Once either condition is hit, the data is flushed to the output
///
/// # Limitations
///
/// This writer does not take into account the actual parquet bytes in
/// the underlying writer (it only tracks `RecordBatch` memory)
///
/// # Background
///
/// Internally, the [`ArrowWriter`] buffers data until it has
/// `set_max_row_group_size` rows before it encodes the data.
///
/// This often works well and results in well encoded parquet
/// files. However, for some datasets the buffering requires
/// substantial (10s of GB) memory.  For more details, see
/// <https://github.com/influxdata/influxdb_iox/issues/7783>
///
/// # Future Improvements:
///
/// See <https://github.com/apache/arrow-rs/issues/3871>
///
/// When this ticket for improving the arrow-rs behavior is complete,
/// this code can probably be improved (to delegate the buffering to
/// the ArrowWriter and just track its memory use with the pool).
pub struct TrackedMemoryArrowWriter<W: Write + Send> {
    /// The inner ArrowWriter
    inner: ArrowWriter<W>,
    /// Buffered data waiting for write. Data is pushed on front, popped from back
    buffer: VecDeque<RecordBatch>,
    /// current bytes buffered
    buffer_size: usize,
    /// current number of rows buffered
    buffer_rows: usize,
    /// maximum number of bytes to buffer
    max_buffer_size: usize,
    /// maximum number of row groups
    max_buffer_rows: usize,
    /// Minimum size to allocate in each increment
    min_allocation_increment: usize,
    /// Memory manager reservation with DataFusion
    reservation: MemoryReservation,
}

// ArrowWriter doesn't implement Debug
impl<W: Write + Send> Debug for TrackedMemoryArrowWriter<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrackedMemoryArrowWriter<W>")
            .field("inner", &"ArrowWriter<W>")
            .field("buffer", &"<buffer>")
            .field("buffer_size", &self.buffer_size)
            .field("buffer_rows", &self.buffer_rows)
            .field("max_buffer_size", &self.max_buffer_size)
            .field("max_buffer_rows", &self.max_buffer_rows)
            .field("min_allocation_increment", &self.min_allocation_increment)
            .field("reservation", &self.reservation)
            .finish()
    }
}

impl<W: Write + Send> TrackedMemoryArrowWriter<W> {
    /// create a new `LimitedBufferingParquetWriter`
    pub fn try_new(
        sink: W,
        schema: SchemaRef,
        props: WriterProperties,
        pool: Arc<dyn MemoryPool>,
    ) -> Result<Self> {
        let max_row_group_size = props.max_row_group_size();
        let inner = ArrowWriter::try_new(sink, schema, Some(props))?;
        let consumer = MemoryConsumer::new("IOx ParquetWriter (TrackedMemoryArrowWriter)");
        let reservation = consumer.register(&pool);

        Ok(Self {
            inner,
            buffer: VecDeque::new(),
            buffer_size: 0,
            buffer_rows: 0,
            max_buffer_size: MAX_BUFFER_SIZE,
            max_buffer_rows: max_row_group_size,
            min_allocation_increment: MIN_ALLOCATION_INCREMENT,
            reservation,
        })
    }

    /// Set the maximum amount of data that will be buffered before a flush.
    ///
    /// Defaults to [`MAX_BUFFER_SIZE`]
    ///
    /// The writer will force a flush when it has buffered this amount
    /// of memory, even if it could get more memory from the memory
    /// pool
    pub fn with_max_buffer_size(mut self, max_buffer_size: usize) -> Self {
        self.max_buffer_size = max_buffer_size;
        self
    }

    /// Set the maximum number of rows that will be buffered before a flush.
    ///
    /// The writer will force a flush once it has buffered this many
    /// rows, regardless of the size of its buffer
    pub fn with_max_buffer_rows(mut self, max_buffer_rows: usize) -> Self {
        self.max_buffer_rows = max_buffer_rows;
        self
    }

    /// Set the minimum allocation increment size.
    ///
    /// The writer will always allocate at least this much additional
    /// space from the memory pool.
    pub fn with_min_allocation_increment(mut self, min_allocation_increment: usize) -> Self {
        self.min_allocation_increment = min_allocation_increment;
        self
    }

    /// Returns the total size, in bytes, of the buffered data
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    /// Push a `RecordBatch` into the buffer, flushing if we can't
    /// allocate sufficient space from the memory manager
    pub fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let batch_rows = batch.num_rows();
        let batch_size = batch.get_array_memory_size();

        // enforce the limits, if needed
        if self.buffer_size + batch_size > self.max_buffer_size
            || self.buffer_rows + batch_rows > self.max_buffer_rows
        {
            self.flush()?;
        }
        self.buffer_rows += batch_rows;
        self.buffer_size += batch_size;
        self.ensure_reservation()?;

        // add batch to queue
        self.buffer.push_back(batch);
        Ok(())
    }

    /// Ensures that our reservation with the memory pool is
    /// sufficiently large for the amount of data that is currently
    /// buffered. If not, attempts to grow the reservation to match
    /// what is currently buffered.
    pub fn ensure_reservation(&mut self) -> Result<()> {
        if self.reservation.size() < self.buffer_size {
            let increment =
                (self.buffer_size - self.reservation.size()).max(self.min_allocation_increment);

            debug!(
                increment,
                rows = self.buffer_rows,
                max_buffer_rows = self.max_buffer_rows,
                buffer_size = self.buffer_size,
                max_buffer_size = self.max_buffer_size,
                "Attemping to reserve additional space"
            );

            self.reservation
                .try_grow(increment)
                .map_err(Error::OutOfMemory)?
        }

        Ok(())
    }

    /// flushes all buffered [`RecordBatch`] to the underlying writer.
    pub fn flush(&mut self) -> Result<()> {
        debug!(
            rows = self.buffer_rows,
            max_buffer_rows = self.max_buffer_rows,
            buffer_size = self.buffer_size,
            max_buffer_size = self.max_buffer_size,
            reservation = self.reservation.size(),
            "Flushing data to parquet"
        );

        while let Some(batch) = self.buffer.pop_front() {
            self.inner.write(&batch)?;
        }
        self.inner.flush()?;
        self.buffer_rows = 0;
        self.buffer_size = 0;
        // Note don't return the reservation once the data is
        // flushed. Instead the writer holds on to the reservation as
        // the expectation is we will be getting more data to buffer
        // soon
        Ok(())
    }

    /// closes the writer, flushing any remaining data and returning
    /// the written [`FileMetaData`]
    ///
    /// [`FileMetaData`]: parquet::format::FileMetaData
    pub fn close(mut self) -> Result<parquet::format::FileMetaData> {
        self.flush()?;
        Ok(self.inner.close()?)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::array::{ArrayRef, StringArray};
    use datafusion::execution::memory_pool::GreedyMemoryPool;

    /// Number of rows to trigger writer flush
    const TEST_MAX_ROW_GROUP_SIZE: usize = 100;
    /// Allocation increments
    const TEST_INCREMENT_SIZE: usize = 1000; // allocate in multiples of 1000
    /// Memory (multiple of increment size) for 100 rows (at time of
    /// writing each 10 row batch takes up 248 bytes so 10 batches
    /// need 2480 --> rounded up to 3000)
    const MEMORY_NEEDED_FOR_100_ROWS: usize = 3000;

    #[tokio::test]
    async fn test_pool_allocation() {
        let props = WriterProperties::builder()
            .set_max_row_group_size(TEST_MAX_ROW_GROUP_SIZE)
            .build();

        let pool = memory_pool(10000);
        let mut writer =
            TrackedMemoryArrowWriter::try_new(vec![], batch().schema(), props, Arc::clone(&pool))
                .unwrap()
                .with_min_allocation_increment(TEST_INCREMENT_SIZE);

        let mut batches = StreamGenerator::new();

        // feed 9 batches in (right before row group max)
        for _ in 0..9 {
            writer.write(batches.next_batch()).unwrap();
        }

        // The writer wrote in several batches
        // expect that the memory reservation is the minimum incremented
        assert_eq!(writer.buffer_size(), batches.total_mem);
        assert_eq!(pool.reserved(), MEMORY_NEEDED_FOR_100_ROWS);

        // Feed in 2 more batches (that exceed the max_group_size of
        // 11 and expect a flush happens)
        writer.write(batches.next_batch()).unwrap();
        writer.write(batches.next_batch()).unwrap();

        assert!(
            writer.buffer_size() < batches.total_mem,
            "buffered size {} should be less than generated size {}",
            writer.buffer_size(),
            batches.total_mem
        );
        // reservation should not to up or down (the reservation is not released)
        assert_eq!(pool.reserved(), MEMORY_NEEDED_FOR_100_ROWS);

        // feed in 50 more batches, and expect that the reservation
        // doesn't need to go up (it is reused)
        for _ in 0..50 {
            writer.write(batches.next_batch()).unwrap();
        }
        assert_eq!(pool.reserved(), MEMORY_NEEDED_FOR_100_ROWS);
    }

    #[tokio::test]
    async fn test_pool_memory_pressure() {
        let props = WriterProperties::builder()
            .set_max_row_group_size(TEST_MAX_ROW_GROUP_SIZE)
            .build();

        // Use a smaller pool size that can't fit all 100 rows, but also limit the writer to that limit as well
        let pool_size = 2000;
        assert!(pool_size < MEMORY_NEEDED_FOR_100_ROWS);
        let pool = memory_pool(pool_size);
        let mut writer =
            TrackedMemoryArrowWriter::try_new(vec![], batch().schema(), props, Arc::clone(&pool))
                .unwrap()
                .with_max_buffer_size(pool_size / 2)
                .with_min_allocation_increment(TEST_INCREMENT_SIZE);

        let mut batches = StreamGenerator::new();

        // Should be able to write more than 10 batches without error
        // as the writer will be flushing more frequently
        for _ in 0..20 {
            writer.write(batches.next_batch()).unwrap();
        }

        // The writer wrote in several batches
        // expect that the memory reservation was kept at the allocation limit (pool_size / 2) = 1000
        assert_eq!(pool.reserved(), pool_size / 2);
    }

    #[tokio::test]
    async fn test_pool_oom() {
        let props = WriterProperties::builder()
            .set_max_row_group_size(TEST_MAX_ROW_GROUP_SIZE)
            .build();

        // Pool can only grow to 2000, but we need 3000 to buffer enough batches to get to 100 rows
        let pool_size = 2000;
        assert!(pool_size < MEMORY_NEEDED_FOR_100_ROWS);
        let pool = memory_pool(pool_size);
        let mut writer =
            TrackedMemoryArrowWriter::try_new(vec![], batch().schema(), props, Arc::clone(&pool))
                .unwrap()
                .with_min_allocation_increment(TEST_INCREMENT_SIZE);

        let mut batches = StreamGenerator::new();

        // should error with oom
        for _ in 0..20 {
            match &writer.write(batches.next_batch()) {
                Ok(_) => continue,
                Err(Error::OutOfMemory(e)) => {
                    assert_eq!("Resources exhausted: Failed to allocate additional 1000 bytes for IOx ParquetWriter (TrackedMemoryArrowWriter) with 2000 bytes already allocated - maximum available is 0", e.to_string());
                    return;
                }
                Err(e) => {
                    panic!("unexpected error: {e})");
                }
            }
        }
        panic!("Did not error with out of memory as expected");
    }

    // This time test that the memory limit is enforced and it will stay under the limit

    #[derive(Debug, Clone, PartialEq)]
    struct StreamGenerator {
        num_rows: usize,
        num_batches: usize,
        total_mem: usize,
    }

    impl StreamGenerator {
        fn new() -> Self {
            Self {
                num_rows: 0,
                num_batches: 0,
                total_mem: 0,
            }
        }

        fn next_batch(&mut self) -> RecordBatch {
            let batch = batch();
            self.num_batches += 1;
            self.num_rows += batch.num_rows();
            self.total_mem += batch.get_array_memory_size();
            batch
        }
    }

    fn batch() -> RecordBatch {
        RecordBatch::try_from_iter([("a", string_array(10))]).unwrap()
    }

    /// Makes a string array with `count` entries
    fn string_array(count: usize) -> ArrayRef {
        let array: StringArray = std::iter::repeat(Some("foo")).take(count).collect();
        Arc::new(array)
    }

    /// make a MemoryPool with the specified max size
    fn memory_pool(max_size: usize) -> Arc<dyn MemoryPool> {
        Arc::new(GreedyMemoryPool::new(max_size))
    }
}
