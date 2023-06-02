# Deduplication

In an IOx table (aka measurement), data with the same primary key are considered duplicates. The primary key of a table is a set of all `tag` columns and the `time` column of that table. Duplicates can have different values of `field` columns and deduplication process is needed to only keep the most recently inserted field values.

-Data **deduplication** happens in `Ingester`, `Compactor`, and `Querier` using the [same code base](https://github.com/influxdata/influxdb_iox/blob/2d2c3d5f8b9968ee7ba03cd4a2d7d2e7e8f30066/iox_query/src/provider.rs#L196). In order to deduplicate chunks, their data must be sorted on `the same sort order`, and that sort order must include all columns of their primary key. We define this sort order to be the **sort key**. Since existing chunks can be deduplicated again and again with newly ingested chunks, maintaining a consistent sort order for chunks which may contain duplicated data helps avoid resorting them.

This document explains how data chunks are deduplicated and sorted consistently to avoid resort.

### Overlapped Chunks

Table data is represented in one of two ways in an IOx physical plan:
- `RecordBatchesExec`, representing data in the ingester that has not yet been persisted to the object store, in the form of Arrow record batches. It is *not* sorted on the primary key, and *may contain duplicates within itself*.
- `ParquetExec`, representing a set of parquet files that have been persisted to an object store. Each individual file will *not* contain duplicates, but there may be duplicates in files that overlap in their time ranges. The output stream of `ParquetExec` will be sorted on the primary key for the table.

In IOx, a "chunk" is an abstraction that represents either Arrow record batches, or parquet files. The [`QueryChunk`](https://github.com/influxdata/influxdb_iox/blob/2d2c3d5f8b9968ee7ba03cd4a2d7d2e7e8f30066/iox_query/src/lib.rs#L234) trait has implementations for both of these formats.

Only chunks that might contain the same primary key might contain duplicates. This means that chunks with overlapped time ranges may potentially have duplicates. Since IOx partitions data by time (e.g. one day per partition), chunks in different partitions are guaranteed not to overlap. Figure 1 shows an example of two partitions. Each has four chunks respectively. Two of them overlap with each other, the other two do not overlap with any but one has duplicates within the chunk itself and the other has no duplicates at all.

```text
╔══════════════════════════════════════════════════════════════════╗
║                     PARTITION 1 (2022.04.25)                     ║
║ ┌──────────────────┐                                             ║
║ │  Parquet P1.1    │                                             ║
║ │Overlaps with P1.2│                                             ║
║ └──────────────────┘                                             ║
║     ┌──────────────────┐ ┌─────────────────┐ ┌─────────────────┐ ║
║     │ Parquet P1.2     │ │RecordBatches R1 │ │ Parquet P1.4    │ ║
║     │Overlaps with P1.1│ │Duplicates within│ │  No Duplicates  │ ║
║     └──────────────────┘ └─────────────────┘ └─────────────────┘ ║
╚══════════════════════════════════════════════════════════════════╝
╔══════════════════════════════════════════════════════════════════╗
║                     PARTITION 2 (2022.04.24)                     ║
║ ┌──────────────────┐                                             ║
║ │  Parquet P2.1    │                                             ║
║ │Overlaps with P2.2│                                             ║
║ └──────────────────┘                                             ║
║     ┌──────────────────┐ ┌─────────────────┐ ┌─────────────────┐ ║
║     │  Parquet P2.2    │ │RecordBatches R2 │ │  Parquet P2.4   │ ║
║     │Overlaps with P2.1│ │Duplicates within│ │  No Duplicates  │ ║
║     └──────────────────┘ └─────────────────┘ └─────────────────┘ ║
╚══════════════════════════════════════════════════════════════════╝
Figure 1: Two partitions, each has four chunks
```

### Data Deduplication

-Figure 2 shows a simplified query plan that IOx builds (in Ingester, Compactor and Querier using the same code based pointed out above) to read all eight chunks of these two partitions and deduplicate them to return only non-duplicated data. This diagram should be read bottom up as follows:
- First, IOx scans eight chunks of the two partitions.
- Chunks P1.1 and P1.2 overlap, they need to get deduplicated. Same to chunks P2.1 and P2.2. To do this, chunks P1.1 and P1.2 are sorted on the same sort key K1.1_2 (see below for how it is computed) and the Dedup (deduplication) is applied to eliminate duplicated values. Similarly, chunks P2.1 and P2.2 are sorted on the same K2.1_2 sort key and then deduplicated.
- Chunk R1 does not overlap with any chunks but potentially has duplicates in itself, hence it also needs to get sorted on its sort key K1.3 and then deduplicated. Same to chunk R2.
- Chunk P1.4 and P2.4 do not overlap with any other chunks and each contain no duplicates, no deduplication is needed for them hence they do not need to get sorted either.
- All the data is then unioned and returned.

Here is [the code](https://github.com/influxdata/influxdb_iox/blob/2d2c3d5f8b9968ee7ba03cd4a2d7d2e7e8f30066/iox_query/src/provider.rs#L196) builds the initial physical plan for a table scan.

```text
 ┌────────────────────────────────────────────────────────────────────────────────┐ 
 │                                     Union                                      │ 
 └────────────────────────────────────────────────────────────────────────────────┘ 
         ▲                      ▲                  ▲         ▲         ▲        ▲   
         │                      │                  │         │         │        │   
┌─────────────────┐    ┌─────────────────┐     ┌──────┐  ┌──────┐      │        │   
│      Dedup      │    │      Dedup      │     │Dedup │  │Dedup │      │        │   
└─────────────────┘    └─────────────────┘     └──────┘  └──────┘      │        │   
    ▲          ▲           ▲          ▲            ▲         ▲         │        │                     
    │          │           │          │            │         │         │        │                      
........   ........    ........   ........     ........  ........      │        │                      
│ Sort │   │ Sort │    │ Sort │   │ Sort │     │ Sort │  │ Sort │      │        │                      
│K1.1_2│   │K1.1_2│    │K2.1_2│   │K2.1_2│     │ K1.3 │  │ K2.3 │      │        │                      
........   ........    ........   ........     ........  ........      │        │                      
    ▲          ▲           ▲          ▲            ▲         ▲         │        │                      
    │          │           │          │            │         │         │        │                      
┌──────┐   ┌──────┐    ┌──────┐   ┌──────┐     ┌──────┐  ┌──────┐   ┌──────┐ ┌──────┐               
│ P1.1 │   │ P1.2 │    │ P2.1 │   │ P2.1 │     │ R1   │  │ R2   │   │ P1.4 │ │ P2.4 │
└──────┘   └──────┘    └──────┘   └──────┘     └──────┘  └──────┘   └──────┘ └──────┘                

Figure 2: A simplified query plan that scans all eight chunks of two partitions and deduplicate their data                 
```

If many queries read these two partitions, IOx has to build the same query plan above which we think suboptimal because of sort and deduplication. To avoid this suboptimal query plan, IOx is designed to:
1. Data in each partition is frequently compacted to only contain a few non-overlapped and non-duplicated chunks as shown in Figure 3 which will help scan fewer chunks and eliminate the `Sort` and `Dedup` operators as illustrated in Figure 4.
2. All **persisted chunks of the same partition are sorted on the same key** to avoid resorting before deduplication. So the plan in Figure 2 is only seen sometimes in IOx (mostly in Ingester and Querier with data sent back from Ingester), instead, you will mostly see the plan in Figure 4 (for data in Figure 3) or in Figure 6 (for data of Figure 5)

```text
╔═════════════════════════════════════════╗ ╔════════════════════════╗
║        PARTITION 1 (2022.04.25)         ║ ║PARTITION 2 (2022.04.24)║
║ ┌──────────────────┐┌──────────────────┐║ ║  ┌──────────────────┐  ║
║ │      P1.12       ││      P1.34       │║ ║  │     P2.1234      │  ║
║ │  No duplicates   ││  No duplicates   │║ ║  │  No duplicates   │  ║
║ └──────────────────┘└──────────────────┘║ ║  └──────────────────┘  ║
╚═════════════════════════════════════════╝ ╚════════════════════════╝
Figure 3: Compacted Partitions 1 and 2
```
```text                                                        
               ┌──────────────────────────────┐                       
               │            Union             │                       
               └──────────────────────────────┘                       
                    ▲          ▲           ▲                          
                    │          │           │                          
                ┌──────┐   ┌──────┐    ┌──────┐                       
                │P1.12 │   │P1.34 │    │P2.123│
                └──────┘   └──────┘    └──────┘     
Figure 4: Scan plan of the compacted Partitions 1 and 2 of Figure 3    

```
```text
╔════════════════════════════════════════╗  ╔═════════════════════════╗
║        PARTITION 1 (2022.04.25)        ║  ║PARTITION 2 (2022.04.24) ║
║ ┌────────────────────┐                 ║  ║  ┌──────────────────┐   ║
║ │    Parquet P1.1    │                 ║  ║  │Parquet  P2.1234  │   ║
║ │       Sorted       │                 ║  ║  │      Sorted      │   ║
║ │Overlap with P1.234 │                 ║  ║  │  No duplicates   │   ║
║ └────────────────────┘                 ║  ║  └──────────────────┘   ║
║             ┌────────────────────────┐ ║  ╚═════════════════════════╝
║             │    Parquet  P1.234     │ ║
║             │         Sorted         │ ║                             
║             │   Overlap with P1.1    │ ║
║             └────────────────────────┘ ║                             
╚════════════════════════════════════════╝                             
Figure 5: Two partitions, one is fully compacted into one sorted chunk, the other is partially compacted into two sorted overlapped chunks
```
```text
┌───────────────────────────┐
│           Union           │
└───────────────────────────┘
         ▲              ▲    
         │              │    
┌─────────────────┐ ┌───────┐
│      Dedup      │ │P2.1234│
└─────────────────┘ └───────┘
    ▲          ▲             
    │          │             
┌──────┐   ┌──────┐          
│ P1.1 │   │P1.234│
└──────┘   └──────┘          
Figure 6: Scan plan of data in Figure 5

```

### Sorted and Unsorted Chunks
- Parquet files are always sorted and contain no duplicates within themselves
- Arrow record batches created by the ingester are not sorted and may contain duplicates

# Sort Keys
Due to supporting schema-on-write, it may be the case that parquet files for a table have different sets of columns for that table.  To keep the sort key consistent for all chunks in the same partition, IOx stores two kinds of sort keys:
1. **File Sort Key**: this is the sort key of the file and only includes tag and time columns of that file.
2. **Partition Sort Key**: this is the sort key of all chunks of the same partition. This sort key will include time and all tag columns of all files.

### Compute Partition Sort Key and File Sort Key
- For the first file of the partition, its File Sort Key will be computed based on the ascending of cardinality of the tag columns available in that file. Time column is always last. This File Sort Key will be stored with the file. Since there is only one file at this time, the Partition Sort Key is the same as the File Sort Key and stored in the Partition catalog object for that file.
- For the following file, its File Sort Key will be the same as the Partition Sort Key but
  - If the file does not include certain tags in the Partition Sort Key, its File Sort Key will be the Partition Sort Key minus the missing columns.
  - If the file include extra tags that do not exist in the Partition Sort Key, those tags will be added to the end but in front of the time column. In this case, the Partition Sort Key is changed and must be updated to the Partition catalog of the file.

**Example**

In the example below, files in lower numbers are persisted to parquet files before the ones with higher numbers.

```text
         | Tags of File  |   File Sort Key  | Partition Sort Key
---------|---------------|------------------|---------------------------------------------------
File 1   | tag1          | tag1, time       | tag1, time (need catalog insert)
File 2   | tag2          | tag2, time       | tag1, tag2, time (need catalog update)
File 3   | tag1, tag3    | tag1, tag3, time | tag1, tag2, tag3, time (need catalog update)
File 4   | tag2, tag3    | tag2, tag3, time | tag1, tag2, tag3, time
File 5   | tag1, tag4    | tag1, tag4, time | tag1, tag2, tag3, tag4, time (need catalog update)
```

Assuming File 2 and File 3 overlap. As explained above, a query that needs to read those two files have to deduplicate them. Their sort key will be read from the Partition Sort Key which is `tag1, tag2, tag3, tag4, time`. Since those files do not include `tag4`, it will be eliminated and the actually sort key of those 2 files is `tag1, tag2, tag3, time`. To ensure those two files have the same columns, IOx adds missing columns with all NULL values while scanning them(`tag1` and `tag3` to File 2 and `tag2` to File 3). Even though adding NULL columns `tag1` and `tag3` to File 2 makes its primary key and sort key now `tag1, tag2, tag3, time`, it is still sorted as it was on its original `tag2, time` only. Hence no sorted needed for File 2. Same to File 3. Figure 7 shows the query plan that reads File 2 and File 3.

```text
                  ┌────────────────────────────────────────────────────────────────┐                 
                  │                             Dedup                              │                 
                  └────────────────────────────────────────────────────────────────┘                 
                        ▲                                                   ▲                        
                        │                                                   │                        
                        │                                                   │                        
┌───────────────────────────────────────────────┐   ┌───────────────────────────────────────────────┐
│                    File 2                     │   │                    File 3                     │
│                                               │   │                                               │
│         . Was sorted on: `tag2, time`         │   │      . Was sorted on: `tag1, tag3, time`      │
│                                               │   │                                               │
│. Is still sorted on: `tag1, tag2, tag3, time` │   │. Is still sorted on: `tag1, tag2, tag3, time` │
│    because tag1 and tag3 contain all NULLs    │   │        because tag2 contains all NULLs        │
└───────────────────────────────────────────────┘   └───────────────────────────────────────────────┘
Figure 7: Scan plan that reads File 2 and File 3
```

# Planning

Data **deduplication** is performed by the [`DeduplicateExec`](https://github.com/influxdata/influxdb_iox/blob/7a051e2afb80e77b138b1c0ac5a4ee89c7a97cb5/iox_query/src/provider/deduplicate.rs#L31) operator. This operator takes a single input and expects rows to be sorted on the primary key. Inputs will be the output streams of `ParquetExec` and/or `RecordBatchesExec` operators (possibly sorted and/or unioned together).

IOx-specific physical optimizers ensure that the naive physical plans are as performant as possible:
- Sort only when necessary
- Deduplicate only when necessary

See [IOx Physical Plan Construction](./physical_plan_construction.md) for a more detailed discussion.

## Services

### Ingester

Within an ingester, a logical plan to ensure persisted writes are deduplicated is created by [`ReorgPlanner::compact_plan`](https://github.com/influxdata/influxdb_iox/blob/7a051e2afb80e77b138b1c0ac5a4ee89c7a97cb5/iox_query/src/frontend/reorg.rs#L72). This planner is invoked on a set of Arrow record batches created from recent writes, and creates a plan that will sort and deduplicate before serializing the output to a parquet file and persisting it.

### Compactor

Compacters also make use of [`ReorgPlanner::compact_plan`](https://github.com/influxdata/influxdb_iox/blob/7a051e2afb80e77b138b1c0ac5a4ee89c7a97cb5/iox_query/src/frontend/reorg.rs#L72) to compact parquet files that may overlap.

For more details on compactors, see [Job of a Compactor](./compactor.md).

### Querier

Queriers use the typical DataFusion code path of creating a logical plan from a SQL statement, using `ChunkTableProvider` to scan IOx tables. As with the other services, table scans are translated to the correct physical operators in the physical plan.

