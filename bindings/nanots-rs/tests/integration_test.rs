// tests/integration_test.rs
use nanots_rs::{Writer, Reader, Iterator, ErrorCode};
use tempfile::NamedTempFile;

#[test]
fn test_basic_write_read_cycle() {
    let temp_file = NamedTempFile::new().unwrap();
    let file_path = temp_file.path().to_str().unwrap();
    
    // Allocate file
    Writer::allocate_file(file_path, 1024 * 1024, 10).unwrap();
    
    // Write data
    {
        let writer = Writer::new(file_path, false).unwrap();
        let context = writer.create_context("test_stream", "test metadata").unwrap();
        writer.write(&context, b"hello world", 1000, 0).unwrap();
        writer.write(&context, b"goodbye world", 2000, 1).unwrap();
    }
    
    // Read data back
    {
        let reader = Reader::new(file_path).unwrap();
        let mut records = Vec::new();
        
        reader.read("test_stream", 0, i64::MAX, |data, flags, timestamp, _block_seq| {
            records.push((data.to_vec(), flags, timestamp));
        }).unwrap();
        
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].0, b"hello world");
        assert_eq!(records[0].1, 0);
        assert_eq!(records[0].2, 1000);
        assert_eq!(records[1].0, b"goodbye world");
        assert_eq!(records[1].1, 1);
        assert_eq!(records[1].2, 2000);
    }
}

#[test]
fn test_iterator_interface() {
    let temp_file = NamedTempFile::new().unwrap();
    let file_path = temp_file.path().to_str().unwrap();
    
    // Allocate and write test data
    Writer::allocate_file(file_path, 1024 * 1024, 10).unwrap();
    {
        let writer = Writer::new(file_path, false).unwrap();
        let context = writer.create_context("iter_test", "iterator test").unwrap();
        
        for i in 0..5 {
            let data = format!("item_{}", i);
            writer.write(&context, data.as_bytes(), i * 1000, 0).unwrap();
        }
    }
    
    // Test iterator
    {
        let mut iter = Iterator::new(file_path, "iter_test").unwrap();
        iter.reset().unwrap();
        
        let mut count = 0;
        while iter.is_valid() {
            let frame = iter.current_frame().unwrap();
            let expected_data = format!("item_{}", count);
            assert_eq!(frame.data, expected_data.as_bytes());
            assert_eq!(frame.timestamp, count * 1000);
            
            iter.next().unwrap();
            count += 1;
        }
        
        assert_eq!(count, 5);
    }
}

#[test]
fn test_find_functionality() {
    let temp_file = NamedTempFile::new().unwrap();
    let file_path = temp_file.path().to_str().unwrap();
    
    // Allocate and write test data
    Writer::allocate_file(file_path, 1024 * 1024, 10).unwrap();
    {
        let writer = Writer::new(file_path, false).unwrap();
        let context = writer.create_context("find_test", "find test").unwrap();
        
        // Write data at specific timestamps
        let timestamps = [1000, 2000, 3000, 5000, 8000];
        for (i, &timestamp) in timestamps.iter().enumerate() {
            let data = format!("data_{}", i);
            writer.write(&context, data.as_bytes(), timestamp, 0).unwrap();
        }
    }
    
    // Test find functionality
    {
        let mut iter = Iterator::new(file_path, "find_test").unwrap();
        
        // Find timestamp that exists
        assert!(iter.find(3000).unwrap());
        let frame = iter.current_frame().unwrap();
        assert_eq!(frame.timestamp, 3000);
        assert_eq!(frame.data, b"data_2");
        
        // Find timestamp between existing ones (should find next)
        assert!(iter.find(4000).unwrap());
        let frame = iter.current_frame().unwrap();
        assert_eq!(frame.timestamp, 5000);
        assert_eq!(frame.data, b"data_3");
        
        // Find timestamp before any data
        assert!(iter.find(500).unwrap());
        let frame = iter.current_frame().unwrap();
        assert_eq!(frame.timestamp, 1000);
        assert_eq!(frame.data, b"data_0");
    }
}

#[test]
fn test_contiguous_segments() {
    let temp_file = NamedTempFile::new().unwrap();
    let file_path = temp_file.path().to_str().unwrap();
    
    // Allocate and write test data
    Writer::allocate_file(file_path, 1024 * 1024, 10).unwrap();
    {
        let writer = Writer::new(file_path, false).unwrap();
        let context = writer.create_context("segment_test", "segment test").unwrap();
        writer.write(&context, b"test data", 1000, 0).unwrap();
    }
    
    // Query segments
    {
        let reader = Reader::new(file_path).unwrap();
        let segments = reader.query_contiguous_segments("segment_test", 0, i64::MAX).unwrap();
        
        // Should have at least one segment
        assert!(!segments.is_empty());
        assert!(segments[0].start_timestamp <= 1000);
        assert!(segments[0].end_timestamp >= 1000);
    }
}

#[test]
fn test_error_handling() {
    // Test opening non-existent file
    let result = Reader::new("/non/existent/file.nts");
    assert!(result.is_err());
    if let Err(error_code) = result {
        assert_eq!(error_code, ErrorCode::CantOpen);
    }
    
    // Test iterator on non-existent file
    let result = Iterator::new("/non/existent/file.nts", "test");
    assert!(result.is_err());
    if let Err(error_code) = result {
        assert_eq!(error_code, ErrorCode::CantOpen);
    }
}

#[test]
fn test_multiple_streams() {
    let temp_file = NamedTempFile::new().unwrap();
    let file_path = temp_file.path().to_str().unwrap();
    
    // Allocate file
    Writer::allocate_file(file_path, 1024 * 1024, 20).unwrap();
    
    // Write to multiple streams
    {
        let writer = Writer::new(file_path, false).unwrap();
        let ctx1 = writer.create_context("stream1", "first stream").unwrap();
        let ctx2 = writer.create_context("stream2", "second stream").unwrap();
        
        writer.write(&ctx1, b"stream1_data1", 1000, 0).unwrap();
        writer.write(&ctx2, b"stream2_data1", 1500, 0).unwrap();
        writer.write(&ctx1, b"stream1_data2", 2000, 0).unwrap();
        writer.write(&ctx2, b"stream2_data2", 2500, 0).unwrap();
    }
    
    // Read from each stream separately
    {
        let reader = Reader::new(file_path).unwrap();
        
        // Read stream1
        let mut stream1_data = Vec::new();
        reader.read("stream1", 0, i64::MAX, |data, _flags, timestamp, _block| {
            stream1_data.push((data.to_vec(), timestamp));
        }).unwrap();
        
        assert_eq!(stream1_data.len(), 2);
        assert_eq!(stream1_data[0].0, b"stream1_data1");
        assert_eq!(stream1_data[0].1, 1000);
        assert_eq!(stream1_data[1].0, b"stream1_data2");
        assert_eq!(stream1_data[1].1, 2000);
        
        // Read stream2
        let mut stream2_data = Vec::new();
        reader.read("stream2", 0, i64::MAX, |data, _flags, timestamp, _block| {
            stream2_data.push((data.to_vec(), timestamp));
        }).unwrap();
        
        assert_eq!(stream2_data.len(), 2);
        assert_eq!(stream2_data[0].0, b"stream2_data1");
        assert_eq!(stream2_data[0].1, 1500);
        assert_eq!(stream2_data[1].0, b"stream2_data2");
        assert_eq!(stream2_data[1].1, 2500);
    }
}
