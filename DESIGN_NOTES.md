# Design Notes

## File System considerations

- If `Segment.__init__` is responsible for creating the file upon creating an instance of Segment and fails if trying to
create a new segment from an existing segment path, we may run into some issues:
  - DB restoring/restarts. We can't pass a path with existing data and create segment objects to start operating with 
  the data that was written. How would we solve that problem if that was the case? We would need to force the creation
  of segments from an existing db in a different way. 
  - Instantiation of segment. We need to pass the `db_path`, `table_name`, `partition_id` and `segment_id`. With this
  info the `Segment.__init__` will create the file. However, if we know that the segment exists but we don't have the
  handle of the segment object there is no way we can create one. 
- If `Segment.__init__` is not responsible for creating the file upon creating an instance of Segment these are some
ideas about how we would go about it:
  - This would mean that we need a `DBFileSystem` class to help us handle the creation of paths for segments but also, we
could mess up the paths for segments and tables. This file system would be responsible for telling us, for a given table
the existing partitions and segments per partition and also tell us their ids. If we had that, we could create a Segment
with its right file. 
    - Do we need to have a structure in the folders? What happens if we restart the DB? Either we have a
structure that the `DBFileSystem` can understand and infer the tables, partitions and segments, or there is somewhere
else where we store the information of what the structure for the DB is and we can restore the DB handler objects from
it. 
  - The DB classes maybe should be named `PartitionHandler` and `SegmentHandler` as they simply represent a partition or
a segment that has to exists and just help up operate on them.
  - We may need a different interface that holds all the tables that the db contains and communicates with the 
`DBFileSystem`.   