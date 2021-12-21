# IOx Transactions and Locks

To keep data consistent, IOx needs to lock objects that are being modified and group actions that need to either all succeed or all fail into a transaction. This document describes types of locks and transactions that IOx supports. As illustrated in two previous documents, [Data Organization and LifeCycle](data_organization_lifecycle.md) and [Catalog](catalogs.md), IOx separates handling `Physical Data Chunks` from its metadata known as `Catalog`. Since `Catalog` is the core information used to operate the database and points to physical `Data Chunks`, most of the locking  and transaction activities will happen on `Catalog Objects`. However, before digging into Catalog's transactions and locks, let us see how `Physical Data Chunks` are read and written.

## Read and Write Physical Data Chunks
As described in [IOx Data Organization and LifeCycle](data_organization_lifecycle.md), there are four types of `Data Chunks` in IOx: `O-MUB`, `F-MUB`, `RUB`, and `OS` in which only `O-MUB` is mutable, the others are immutable. So when a write is issued, it will go to its corresponding `O-MUB`, but when a read occurs, it may need to read all types of chunks that may contain needed data[^prune].

[^prune]: IOx supports chunk pruning to eliminate chunks that do not contains needed data but it is beyond the scope of this document.

* **Read a `F-MUB`, a `RUB` or a `OS`:** When an immutable data chunk is identified to be read, its reference will be returned without locking because it never gets modified. If IOx wants to remove this chunk, it will wait until no references to the chunk before doing so. Note that the `Catalog Objects` described in Figure 1 of [IOx Catalog](catalogs.md)  such as `Table`, `Partition`, and `Chunk` that lead IOx to this data chunk  will be locked for `read` temporarily while searching for data chunks to ensure the data chunk won't be removed by other concurrent actions. See next section for Catalog locking.

* **Read and Write `O-MUB`:** Unlike immutable data chunks, when a mutable data chunk is identified to be read, a snapshot of its data at that moment will be returned and the query will be run on that snapshot which is isolated from the chunk. This enables IOx to continue running queries if the chunk is then modified to add ingesting data. However, if a read comes while a write is happening, it has to wait to get the next snapshot. IOx plans to improve this limitation in the future by returning the previous snapshot for running the query without waiting for a write to finish.

## Catalog Transactions and Locks

If data is being ingested continuously to IOx, its Data Lifecyle shown in  Figure 3 of [IOx Data Organization and LifeCycle](data_organization_lifecycle.md) will be very changed accordingly to persist data while keeping the queries running fast. The movement of a chunk from one stage and type to another must be handled by its corresponding `Catalog Objects` described in Figure 1 of [IOx Catalog](catalogs.md). Because `Physical Data Chunks` won't be read until they are fully created and linked to its `Catalog Object`, there is no need to lock them as seen in the section above. All transactions and locks are mostly on the `Catalog Objects` instead.

### Flow of Locking for a Read
As explained in Figure 1 of [IOx Catalog](catalogs.md), to reach a `Data Chunk` requested by a query, IOx needs to search through its catalog objects: `Table`, `Partition` and `(Catalog) Chunk`. To avoid the `Data Chunk` getting removed by other concurrent actions that will lead to the modification of the Catalog itself, IOx has to lock its catalog objects. For example, while searching for `RUB 5` and `OS 5`[^search], IOx first locks `Table 1` then `Partition m` then `Chunk 5`. When the references of `RUB 5` and `OS 5` are returned for actual physical data reading, the catalog locks will be released. Since nothing is modified during this process, `read` locks (aka `shared` locks ) are acquired on all catalog objects.

[^search]: the reason to search for specific chunks is based on the predicates of the query and statistics of the chunks that is beyond the scope of this document.

### Flow of Locking for a Write
In principle, whenever we want to write something, we have to acquire `write` lock (aka `exclusive` lock) on related catalog objects. However, since the `write` lock prevents other concurrent actions to access the locked object, the lock holding needs to keep minimal. Let us look into a few examples in the Transaction section below to see when IOx acquires `write` locks.

### Lock Types
Table 1 shows the two common locks and their compatibility. `S` represents `Shared` lock which is a lock for `read`; and `X` represents `Exclusive` lock which is a lock for `write`. While `Thread 1` is holding a lock on an object T, `Thread 2` can only acquired `S` lock on T if the one `Thread 1` is holding is `S` lock. The way we read the table is: `S` lock is compatible with `S` lock but not compatible with `X` lock. `X` lock is not compatible with any locks.

```text

                     \           Thread 1  |  S (read)  | X (write)
         Thread 2     \                    |            |
    ───────────────────────────────────────|────────────|───────────
      Possible to acquire read lock (S)?   |     yes    |   no
    ───────────────────────────────────────|────────────|───────────
      Possible to acquire write lock (X)?  |     no     |   no

    
Table1 1: Common `Shared` and `Exclusive` Locks and Their Compatibility
```

IOx supports one more lock in the middle, `SX`, as shown in Table 2. `SX` is a lock that is first acquired for `read` but will be upgraded to `write` at some point. This allows other concurrent actions to read the `SX`-acquired object before it is upgraded to `write`. However, other concurrent actions won't be able to acquire `SX` or `X` on that object. This means while `Thread 1` is holding `SX` on object T, other threads cannot modify T; and no matter how many threads are holding `S` lock on T while `Thread 1` is holding `SX` on it, `Thread 1` is still able to upgrade its lock to `X` for modifying T.
In this case we say `S` compatible with itself and `SX`, `SX` is only compatible with `S`, and `X` is only compatible with `S` if it is unpgrading from an `SX`.

```text

                     \           Thread 1              |     S (read)      | SX (read-then-write) | X (write)
         Thread 2     \                                |                   |                      
    ───────────────────────────────────────────────────|───────────────────|──────────────────────|───────────
      Possible to acquire read lock (S)?               |     yes           |        yes           |   no
    ───────────────────────────────────────────────────|───────────────────|──────────────────────|───────────
      Possible to acquire read-then-write lock (SX) ?  |     yes?          |        no            |   no
    ───────────────────────────────────────────────────|───────────────────|──────────────────────|───────────
      Possible to acquire write lock (X)               | yes if holding SX |        no            |   no         
                                                       | no otherwise      |                      |
    
Table: IOx extended locks: `Shared`, `Shared Exclusive` and `Exclusive` Locks and Their Compatibility
```


### Transactions
