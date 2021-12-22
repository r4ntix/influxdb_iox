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

### Query a Partition, P1, of a Table, T1
In order to query a table, go need through these major steps:
1. Identifying Data Chunks of the tables
1. Pruning unnecessary Chunks
1. Building a query plan for those Chunks
1. Executing the plan
Since querying only involves reading activities, no transactions are needed. As pointed out above, locks no longer needed when snapshots or references of the Data Chunk are identified, only the first step of the above acquires locks and is very time-trivial compared with the time needed for the other steps.

**Identify Data Chunks of the tables** is implemented in [filtered_chunks](https://github.com/influxdata/influxdb_iox/blob/8a2410e161996603a4147e319c71e5bb38ca9cb7/server/src/db/catalog.rs#L288)[^tables] and as follows:
  1. Acquire `S` lock on all Catalog Tables.
  1. Identify Catalog Table for `T1` and its Catalog Partition `P1`.
  1. Acquire `S` lock on `P1`.
  1. Identify Catalog Chunks for `P1` which are, let say, `C1` and `C2`.
  1. Acquire `S` lock on `C1` and `C2`.
  1. Take snapshots for `C1` and `C2` (snapshot can either be real snapshot for `O-MUB` or reference for `F-MUB`, `RUB` and `OS`)
  1. Release all `S` locks on Chunks (`C1` and `C2`), Partition (`P1`), and Tables.
  1. Return the snapshots.

[^tables]: The filtered_chunks function can return chunks of many tables but we only need chunks of one table in this example.

### Write to an O-MUB Chunk of a Partition, P1, of a Table, T1
A write in IOx is implemented in [store_filtered_write](https://github.com/influxdata/influxdb_iox/blob/ccba68fe3ed3b41f06992cd1c11eefe720ed3ad4/server/src/db.rs#L1053) that can trigger to create a table or partitions as needed but in this example, we only focus on writing data to an available `O-MUB` chunk of a Partition, `P1`, of a Table, `T1`. Similar to reading, the lock acquisition and release are in the same order but on `X` lock instead.
1. Acquire `X` lock on all tables.
1. Identify Catalog Table `T1`
1. Keep `X` lock on `T1`, release locks on other Tables
1. Identify `T1`'s Partition `P1`
1. Acquire `X` lock on `P1`
1. Identify `O-MUB` of `P1` (every partition has max one `O-MUB`)
1. Acquire `X` lock on `O-MUB`
1. Write new data to the `O-MUB`
1. Release `X` lock on `O-MUB`
1. Update `PersistenceWindow` of `P1`
1. Release `X` lock on `P1`
1. Release `X` lock on `T1`

### Compact Chunks to a RUB

### Persist Chunks

### Compact OS Chunks

