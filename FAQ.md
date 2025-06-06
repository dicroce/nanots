**Q: OK, What is NanoTS?**  
**A:** NanoTS is a time series database optimized for raw read and write performance.  

**Q: How fast is it?**  
**A:** Some of our tests show sub microsecond performance for small rows.  

**Q: How durable is it?**  
**A:** Durability is configuarable. "Blocks" are an internal subdivision within
   a NanoTS database. Writers will sync the disk during block transitions, 
   so the size of the block determines your durability. BUT smaller block
   sizes will increase your contention on the block index and slow down your
   writes. We recommend you start with large blocks (50mb) and turn it down
   if you need more durability in practice.  

**Q: How many writers & readers do you allow?**  
**A:** NanoTS allows any numbr of writing threads as long as each is writing to their
   own logical stream + any number of read threads (e.g. 1 writer per stream +
   N readers).  

**Q: What should I know about NanoTS?**  
**A:** Pre allocated storage, blazing fast mostly lock free reads and writes.  

**Q: How to integrate it with your project?**  
**A:** Copy the 4 source files in the amalgamated_src/ dir into your source tree and add it to your build.

**Q: What is up with pre-allocating storage?**  
**A:** NanoTS chooses to pre-allocate its storage space. This may not be a good fit  
   for everyone, but it does have some useful properties:
   - You won't find your server down because your time series database filled
     up the disk.
   - If you are recycling blocks then what you lose is the oldest data when you
     run out of space.
   - Filesystem fragmentation is not an issue.
     
**Q: What does a NanoTS data file look like?**  
**A:** A NanoTS data file consists of a small header plus an array of large fixed
   size blocks. The size of a block is configurable but it should be fairly
   large (10mb is a good size for video data). Besides the data file is a
   secondary file that contains a sqlite database that contains the block
   index.  

**Q: What is in a block?**  
**A:** Each block has a header, an index and a data section. Indexes grow from
   left to right and data grows from right to left. Index entries are fixed
   size and data chunks are variable size. You are out of space in the block
   when the indexes and data would overlap.  

**Q: How many writers and readers can there be?**  
**A:** There can be one writer per write context and any number of readers.  

**Q: What is a write context?**  
**A:** A write context is effectively a stream. Entire blocks are allocated to a stream (so
   while you can have multiple streams being written simultaneously they are being
   written to different blocks). In our schema contiguous streams are called segments.
   Segments have a stream_tag that is provided by the user of the write context that is
   really the type of the stream. When querying data the user must specify a stream_tag
   and a time range. Furthermore the user can provide a metadata string that is also
   associated with the stream (For video, this could be somehing like the SDP of the
   stream).  

**Q: What does deletion look like?**  
**A:** Deletion operates on whole blocks between two timepoints and involves
   updating the database tables rather than modifying block data. The process
   would: 1) begin a sqlite transaction, 2) query segments table joined to
   the segment_blocks table looking for the overlap with the delete range. 3)
   Deleting the segment blocks and marking the deleted blocks as free in the
   blocks table.  

**Q: What about durability and crash recovery?**  
**A:** NanoTS's durability guarantees are around blocks. Larger blocks have more
   read parallelism (since there is less contention on sqlite due to writers
   needing to switch blocks less frequently). Smaller blocks will have less
   potential data loss on crashes or unclean shutdowns.  

**Q: How about a practical configuration? I use nanots to store video streams.**  
**A:** I use 50mb blocks for 1080p h.264 streams. In practice, I've only lost a
   few frames even with my durability guarantees set to 50mb (which is around
   3 minutes of video depening on the bitrate).)  
