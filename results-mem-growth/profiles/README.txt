Heap profiles (jemalloc, build --features heap-prof, lg_prof_sample 16-17), read with `jeprof --text --lines --base=A B`.
The .heap files are not kept (they need the exact binary); the numbers below are the jeprof output of each run.

prof1 - binary main + jemalloc profiler (before the fixes), 50 db x 2 writes/s, dumps every 60 s.
  diff minute 1 -> minute 4 (~18 000 writes): Total 26.4 MB
    20.1 MB  slatedb batch.rs:175 WriteBatch::put -> Bytes::copy_from_slice (key and value kept by the memtable)
     7.1 MB  slatedb mem_table.rs:144 -> crossbeam-skiplist insert (memtable nodes)
     0.2 MB  mongo-server changes.rs (change-stream log, already bounded)
  HEAPPROF allocated: 58.4, 86.7, 98.7, 110.9, 123.5 MB at minutes 1..5

prof2 - same binary, 50 x 2 for 180 s, then every database closes (idle 30 s), dumps every 30 s.
  allocated: 42.1 (start) ... 97.2 (end of writes) ... 60.0 MB (every database closed); jemalloc resident stayed ~93-98 MB
  diff start -> after close: Total 10.8 MB, of which
    19.4 MB  slatedb memtable_flusher/uploader.rs:235 -> db_cache/mod.rs:607 insert (data blocks of flushed L0 SSTs in the
             shared block cache, never read again after the close)
    ~6-11 MB mongo-server changes.rs:133 publish / coll.rs:1103 (documents kept by the change-stream log, bounded)
    (other sites negative: transient buffers of the start sample)

prof3 - fixed binary, test_27 load (20 db x 10/s, memtable age 20 s, change log 4 MB), dumps every 30 s.
  diff 90 s -> 270 s (~36 000 writes): Total 7.4 MB, mostly hyper HTTP/1 read buffers of S3 connections (5.7 MB) and blocks
  read by compactions (foyer get_or_fetch, 1.3 MB): no structure that grows with the writes.

prof4 - fixed binary, 50 x 2 for 45 min, dumps every 300 s: see prof4-15min-vs-40min-diff.txt
  allocated at the dumps (MB): 114, 77, 132, 95, 144, 113, 146, 167, 156
  diff 15 min -> 40 min: Total 38.2 MB, 41.7 MB in hyper poll_read_from_io (read buffers of pooled S3 connections).
