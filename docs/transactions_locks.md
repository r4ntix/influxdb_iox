# IOx Locks and Transactions

In order to keep data consistent, IOx needs to **lock** objects that are being read or modified, and group actions that need to either all succeed or all fail into a **transaction**. This document describes types of locks and transactions that IOx supports. As illustrated in two previous documents, [Data Organization and LifeCycle](data_organization_lifecycle.md) and [Catalog](catalogs.md), IOx separates handling `Physical Data Chunks` from its metadata known as `Catalog`. Since `Catalog` is the core information used to operate the database and points to physical `Data Chunks`, most of the locking  and transaction activities will happen on `Catalog Objects`. However, before digging into Catalog's transactions and locks, let us see how `Physical Data Chunks` are read and written.

## Read and Write Physical Data Chunks
As described in [IOx Data Organization and LifeCycle](data_organization_lifecycle.md), there are four types of `Data Chunks` in IOx: `O-MUB`, `F-MUB`, `RUB`, and `OS` in which only `O-MUB` is mutable, the others are immutable. When a write is issued, it will go to its corresponding `O-MUB`, but when a read occurs, it may need to read all types of chunks that may contain needed data[^prune].

[^prune]: IOx supports chunk pruning to eliminate chunks that do not contains needed data but it is beyond the scope of this document.

* **Read a `F-MUB`, a `RUB` or a `OS`:** When an immutable data chunk is identified to be read, its reference will be returned without locking because it never gets modified. If IOx wants to remove this chunk, it will wait until no references to the chunk before doing so. Next section will describe how the catalog objects that lead IOx to this data chunk is locked.

* **Read and Write `O-MUB`:** Unlike immutable data chunks, when a mutable data chunk is identified to be read, a snapshot of its data at that moment will be returned and the query will be run on that snapshot which is isolated from the chunk. This enables IOx to continue running queries if the chunk is then modified to add ingesting data. However, if a read comes while a write is happening, it has to wait to get the next snapshot. See next section for the detail of catalog locking and transaction on writes.


## Catalog Locks

If data is being ingested continuously to IOx, its Data Lifecyle shown in  Figure 3 of [Data Organization and LifeCycle](data_organization_lifecycle.md) will be changed accordingly to persist data while keeping the queries running fast. The movement of a chunk from one stage and type to another must be handled by its corresponding `Catalog Objects` described in Figure 1 of [Catalog](catalogs.md). Because `Physical Data Chunks` won't be read until they are fully created and linked to its `Catalog Object`, there is no need to lock them as seen in the section above. All transactions and locks are mostly on the `Catalog Objects` instead.

### Flow of Locks for a Read
As explained in Figure 1 of [Catalog](catalogs.md), to reach a `Data Chunk` requested by a query, IOx needs to search through its catalog objects: `Table`, `Partition` and `(Catalog) Chunk`. To avoid the `Data Chunk` from getting removed by other concurrent actions that will lead to the modification of the Catalog itself, IOx has to lock the chunk's catalog objects. More specifically, its `Table` will be locked first, then `Partition`, then `(Catalog) Chunk`. When the `Data Chunk` is identified and its **reference** is returned for actual physical data reading, the acquired locks will be released. Even though the reading actual data chunk can take time depending on how much data chunk has, the lock holding time for this process is just the time to identify the data chunk and very short (sub microsecond)[^search]. Since nothing is modified during this process, `read` locks (defined in next subsection) are acquired on all involved catalog objects.

[^search]: A data chunk is identified based on the predicates of the query and statistics of the chunks that are beyond the scope of this document.

### Flow of Locks for a Write
In principle, whenever we want to write something, we have to acquire `write` lock (defined next) on related catalog objects. However, since the `write` lock prevents other concurrent actions to access the locked object, the lock holding time needs to be kept minimal and customized for each case. The examples below will illustrate when a lock is acquired for specific scenarios.

### Lock Types
Table 1 shows the two common locks and their compatibility. `S` represents `Shared` lock which is a lock for `read`; and `X` represents `Exclusive` lock which is a lock for `write`. 
* A thread holds a **shared (or read) lock** on an object `T` means it is reading `T` and other threads can also read `T` but they are not able to remove or even modify `T`. In some special cases, if a snapshot of `T` was taken for read, one other thread may be able to modify `T` which will be described further below. 
* A thread holds an **exclusive (or write) lock** on an object `T` means it is modifying `T` and no other threads are able to read or modify `T`. If `T` was snapshot by other threads before the write lock was acquired, those threads are still able to read the snapshots while `T` is being modified.

Table 1 explains that while `Thread 1` is holding a lock (`S` or `X`) on an object `T`, `Thread 2` can only acquired `S` lock on `T` if the one `Thread 1` is holding is `S`. The way we read the table:
* `S` lock is compatible with `S` lock (itself) but not compatible with `X` lock.
* `X` lock is not compatible with any locks.

```text

                     \    Thread 1 ->      |    S   |   X 
       Thread 2 |     \                    | (read) |(write)
                V      \                   |        |
    ───────────────────────────────────────|────────|────────
      Possible to acquire read lock (S)?   |  yes   |  no
    ───────────────────────────────────────|────────|────────
      Possible to acquire write lock (X)?  |  no    |  no

    
Table1 1: Common `Shared` and `Exclusive` Locks and Their Compatibility
```

IOx supports one more lock in the middle, `SX`, as shown in Table 2 and implemented in [Freezable](https://github.com/influxdata/influxdb_iox/blob/fa47fb5582cb7527817a8c2834b82b5eb604ad46/internal_types/src/freezable.rs). `SX` is a lock that is first acquired for `read` but will be upgraded to `write` at some point. This allows other concurrent threads to read the `SX`-acquired object before it is upgraded to `write` but won't be able to acquire `SX` or `X` on that object. This means while `Thread 1` is holding `SX` on object `T`, other threads cannot modify T; and no matter how many threads are holding `S` lock on `T` while `Thread 1` is holding `SX` on it, `Thread 1` is still able to upgrade its lock to `X` for modifying `T`.
In this case we say:
* `S` is compatible with itself and `SX`
* `SX` is only compatible with `S`
* `X` is only compatible with `S` if it is upgrading from an `SX`.

IOx also support locking on Rust futures which means a lock won't be actually acquired until its corresponding future happens. 

```text

                     \           Thread 1 ->           |     S               | SX (read-then-write) | X (write)
         Thread 2 |   \                                |  (read)             | (read-then-write)    |
                  V    \                               |                     |                      |
    ───────────────────────────────────────────────────|─────────────────────|──────────────────────|───────────
      Possible to acquire read lock (S)?               |     yes             |        yes           |   no
    ───────────────────────────────────────────────────|─────────────────────|──────────────────────|───────────
      Possible to acquire read-then-write lock (SX) ?  |     yes?            |        no            |   no
    ───────────────────────────────────────────────────|─────────────────────|──────────────────────|───────────
      Possible to acquire write lock (X)               | . yes if holding SX |        no            |   no         
                                                       | . no otherwise      |                      |
    
Table 2: IOx Locks and Their Compatibility
```


## Examples of Locks and Transactions
Let us go over a few common actions in IOx Data Lifecycle to explore when locks and transactions are used.

### Example 1: Query Data of a Table, T1
In order to query a table, we need through these steps:
1. Step 1: Identifying Data Chunks of the table and take their snapshots.
1. Step 2: Pruning unnecessary Chunks
1. Step 3: Building a query plan for those Chunks
1. Step 4: Executing the plan

Since querying only involves reading activities, no transactions are needed. As pointed out above, locks no longer needed when snapshots or references of the Data Chunk are identified, only the first step acquires locks and the subject of discussion of this document.

**Step 1: Identify Data Chunks of the tables and take their snapshots** are implemented in [filtered_chunks](https://github.com/influxdata/influxdb_iox/blob/8a2410e161996603a4147e319c71e5bb38ca9cb7/server/src/db/catalog.rs#L288)[^tables] and as follows:
  1. <span style="color:red">Acquire</span> `S` lock on all Catalog Tables.
  1. Identify Catalog Table of `T1` and, let say, its only appropriate Catalog Partition for this query, `P1`.
  1. <span style="color:red">Acquire</span> `S` lock on `P1`.
  1. Identify Catalog Chunks of `P1` which are, let say, `C1` and `C2`.
  1. <span style="color:red">Acquire</span> `S` lock on `C1` and `C2`.
  1. Take snapshots for `C1` and `C2` (snapshot can either be real snapshot for `O-MUB` or reference for `F-MUB`, `RUB` and `OS`)
  1. <span style="color:green">Release all</span> `S` locks on Chunks (`C1` and `C2`), Partition (`P1`), and Tables.
  1. Return the snapshots.

[^tables]: The filtered_chunks function can return chunks of many tables but we only need chunks of one table in this example.

### Example 2: Write to an O-MUB Chunk of Partition, P1, of Table, T1
A write in IOx is implemented in [store_filtered_write](https://github.com/influxdata/influxdb_iox/blob/ccba68fe3ed3b41f06992cd1c11eefe720ed3ad4/server/src/db.rs#L1053) that can trigger to create a table or partitions as needed but in this example, we only focus on writing data to an available `O-MUB` chunk of Partition, `P1`, of Table, `T1`. Similar to reading, the lock acquisition and release are in the same order but on `X` lock instead.
1. <span style="color:red">Acquire</span> `X` lock on all tables.
1. Identify Catalog Table `T1`
1. <span style="color:red">Keep</span> `X` lock on `T1`, <span style="color:green">release</span>  locks on other Tables
1. Identify `T1`'s Partition `P1`
1. <span style="color:red">Acquire</span> `X` lock on `P1`
1. Identify `O-MUB` of `P1` (every partition has max one `O-MUB`. New `O-MUB` will be created if not avaialble)
1. <span style="color:red">Acquire</span> `X` lock on `O-MUB`
1. Write new data to `O-MUB`
1. <span style="color:green">Release</span> `X` lock on `O-MUB`
1. Update `PersistenceWindow` of `P1` (This is needed for compacting & persisting Chunks)
1. <span style="color:green">Release</span> `X` lock on `P1`
1. <span style="color:green">Release</span> `X` lock on `T1`

### Example 3: Compact Object Store Chunks of Partition, P1
As defined in [Data Organization and LifeCycle](data_organization_lifecycle.md), Compact Object Store Chunks implemented in [compact_object_store_chunk](https://github.com/influxdata/influxdb_iox/blob/56c7e3cd607e4f42c70c06609a427d6196d1d9de/server/src/db/lifecycle/compact_object_store.rs#L63) is an action to compact eligible Object Store Chunks into one Object Store Chunk. This includes the following major steps[^ossteps], each may acquire and release locks.
1. Step 1: Identify Catalog OS Chunks to compact and take their snapshots. This is similar to the first step of `querying a table` described above and will  <span style="color:red">acquire</span> and <span style="color:green">release</span> `S` locks on appropriate Table, Partition, and Chunks.
1. Step 2: Compact the snapshots. This is similar to the third step of `querying a partition` that build a query plan for the snapshots and execute the plan. This step does not take any locks
1. Step 3: Persist the compacted data into a new OS Data Chunk. This requires new a type of lock as well as a transaction and will be described in detail below.
1. Step 4: Update the Catalog to use the newly created OS Data Chunk and remove the compacted Catalog Chunks.


**Step 3: Persist the compacted data into a new OS Data Chunk**

As described in [Catalog](catalogs.md), all OS Data Chunks are durable and should be able to recover in case of disaster. To do so, the information of Catalog Objects of these OS Data Chunks should also be persisted as `Catalog Transactions`. Note that `Catalog Transaction` is different from `Database Transaction` (or simply a `transaction`) mentioned in this document so far. `Catalog Transaction` only records something that has happened and does not trigger those actions. Since the job of Compact OS Chunks is to replace a few OS Data Chunks with a new OS Data Chunk, `Catalog Transactions` of the Catalog Objects of the new Data Chunk are also needed to get saved in the object store. Thus, this step includes two main sub-steps: (i) write the compacted data to a parquet file in the Object Store, and (ii) write the `Catalog Transactions` of the chunk created in sub-step i in the object store in a transaction. Since we do not want the parquet file created in sub-step i to get cleaned up by a background job because it does not have any Catalog Object refers to, we need to lock the `cleanup` lock during this step.  The whole process of this step can be split into many sub-steps as follows:

1. Step a: <span style="color:red">Acquire</span> `S` lock on **cleanup lock** to ensure no cleanup jobs can remove any parquet files.
1. Step b: Write compacted data to a parquet file, `PF`, in the object store.
1. Step c: <span style="color:red">Open</span>  a Catalog Transaction, `CT`
1. Step d: Tell `CT` to remove parquet files of compacted chunks. These are file paths of the OS chunks identified in Step 1 above. Note that the `CT` will not remove anything but just records that those files have been `soft` deleted and should not be used in the future. Those files can still be accessed by some queries, and at this moment the Catalog still use them. See the detailed of Step 4 for when these files are no longer used and can be hard deleted by a background cleanup job.
1. Step e: Tell `CT` to add the newly created  parquet file, `PF`[^tran].
1. Step f: <span style="color:green">Commit</span>  `CT`
1. Step g: <span style="color:green">Release</span> `S` on the **cleanup lock**.

By open and commit a `Catalog Transaction`, IOx guarantees the old parquet files are only removed from the catalog if its newly compacted file is added.

**Step 4: Update the Catalog to use the newly created OS Data Chunk and remove the compacted Catalog Chunks**
After step 3, the newly compacted Data Chunk is fully stored in the Object Store with corresponding `Catalog Transactions` to rebuild the Catalog as needed. However, the current Catalog still contains old Catalog Objects that link to the old OS Data Chunks. This step is to update the Catalog to remove the old Catalog Objects and add the new ones to link to the new OS Data Chunk created in step 3 and as follows:
1. Step i: <span style="color:red">Acquire</span> `X` lock on the provided partition produced in Step 1.
1. Step ii: Drop compacted Catalog Chunks from the partition. These chunks are also provided from Step 1. 
1. Step iii: Create a new Catalog Chunk that links to the OS Data Chunk created in Step 3.
1. Step iv: <span style="color:green">Release</span> X` lock on the Partition.


[^ossteps]: The Compact Object Store also removes delete tombstones that require further locking. Delete is a large topic and deserves its own document.


