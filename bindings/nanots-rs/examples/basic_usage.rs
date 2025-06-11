// examples/basic_usage.rs
use nanots_rs::{Writer, Reader, Iterator};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = "example.nts";
    
    println!("Creating nanots database...");
    
    // Step 1: Allocate the database file
    Writer::allocate_file(file_path, 1024 * 1024, 100)?;
    println!("✓ Allocated database file");
    
    // Step 2: Write some sample data
    {
        let writer = Writer::new(file_path, false)?;
        let context = writer.create_context("sensor_data", "temperature readings")?;
        
        println!("Writing sample data...");
        for i in 0..10 {
            let data = format!("measurement_{}", i);
            writer.write(&context, data.as_bytes(), i * 1000, 0)?;
        }
        println!("✓ Wrote 10 measurements");
    } // writer and context dropped here
    
    // Step 3: Read data back using callback
    {
        println!("\nReading data with callback:");
        let reader = Reader::new(file_path)?;
        let mut count = 0;
        
        reader.read("sensor_data", 0, i64::MAX, |data, flags, timestamp, block_seq| {
            let data_str = String::from_utf8_lossy(data);
            println!("  [{}] {} at timestamp {} (flags: {}, block: {})", 
                     count, data_str, timestamp, flags, block_seq);
            count += 1;
        })?;
        
        println!("✓ Read {} records", count);
    }
    
    // Step 4: Use iterator interface
    {
        println!("\nUsing iterator interface:");
        let mut iter = Iterator::new(file_path, "sensor_data")?;
        iter.reset()?;
        
        let mut count = 0;
        while iter.is_valid() {
            let frame = iter.current_frame()?;
            let data_str = String::from_utf8_lossy(&frame.data);
            println!("  [{}] {} at timestamp {}", count, data_str, frame.timestamp);
            
            iter.next()?;
            count += 1;
        }
        
        println!("✓ Iterated over {} records", count);
    }
    
    // Step 5: Query contiguous segments
    {
        println!("\nQuerying contiguous segments:");
        let reader = Reader::new(file_path)?;
        let segments = reader.query_contiguous_segments("sensor_data", 0, i64::MAX)?;
        
        for (i, segment) in segments.iter().enumerate() {
            println!("  Segment {}: {} -> {} (ID: {})", 
                     i, segment.start_timestamp, segment.end_timestamp, segment.segment_id);
        }
        
        println!("✓ Found {} contiguous segments", segments.len());
    }
    
    // Step 6: Query stream tags
    {
        println!("\nQuerying stream tags:");
        let reader = Reader::new(file_path)?;
        let tags = reader.query_stream_tags(0, i64::MAX)?;
        
        for (i, tag) in tags.iter().enumerate() {
            println!("  Tag {}: {}", i, tag);
        }
        
        println!("✓ Found {} stream tags", tags.len());
    }
    
    println!("\n🎉 All operations completed successfully!");
    println!("Database file: {}", file_path);
    
    Ok(())
}