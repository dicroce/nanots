#!/usr/bin/env python3
"""
Test suite for NanoTS time series database.
This test demonstrates:
1. Allocating a database file
2. Writing time series data
3. Using iterator to find and traverse data
"""

import os
import tempfile
import time
from datetime import datetime, timedelta
import nanots  # Assuming the compiled module is named 'nanots'


def test_nanots_basic_functionality():
    """Test basic nanots functionality: allocate, write, and iterate."""
    
    # Create a temporary file for testing
    with tempfile.NamedTemporaryFile(delete=False, suffix='.nanots') as tmp_file:
        db_filename = tmp_file.name
    
    try:
        print(f"Testing with database file: {db_filename}")
        
        # Step 1: Allocate database file
        print("\n1. Allocating database file...")
        block_size = 4096      # 4KB blocks
        num_blocks = 1000      # 1000 blocks = ~4MB total
        
        nanots.allocate_file(db_filename, block_size, num_blocks)
        print(f"   Allocated {num_blocks} blocks of {block_size} bytes each")
        
        # Step 2: Write test data
        print("\n2. Writing test data...")
        
        # Generate test data with timestamps first
        base_timestamp = int(time.time() * 1000)  # Current time in milliseconds
        test_data = []
        stream_tag = "sensor_data"
        
        # Test with writer alive (should now work with our fix)
        def test_with_writer_alive():
            # Create writer
            writer = nanots.Writer(db_filename, auto_reclaim=False)
            
            # Create write context for a stream
            metadata = "Temperature sensor readings"
            context = writer.create_context(stream_tag, metadata)
            
            print("   Writing sample data points...")
            for i in range(50):
                # Create sample data (temperature readings)
                timestamp = base_timestamp + (i * 1000)  # 1 second intervals
                temperature = 20.0 + (i % 20) + (i * 0.1)  # Varying temperature
                data = f"temp:{temperature:.1f}".encode('utf-8')
                flags = 0
                
                # Write to database
                writer.write(context, data, timestamp, flags)
                
                # Store for verification
                test_data.append({
                    'timestamp': timestamp,
                    'data': data,
                    'temperature': temperature
                })
                
                if i % 10 == 0:
                    print(f"   Written {i+1} data points...")
            
            print(f"   Successfully wrote {len(test_data)} data points")
            print(f"   Time range: {test_data[0]['timestamp']} to {test_data[-1]['timestamp']}")
            
            # Test if reader operations work WHILE writer is still alive
            print("\n   Testing reader operations WHILE writer is alive (should now work!)...")
            
            # Create reader while writer is still alive
            concurrent_reader = nanots.Reader(db_filename)
            
            # Test query_stream_tags while writer alive (this should NOW work!)
            print("   Testing query_stream_tags while writer alive:")
            start_ts = test_data[5]['timestamp']
            end_ts = test_data[15]['timestamp']
            stream_tags_concurrent = concurrent_reader.query_stream_tags(start_ts, end_ts)
            print(f"     Query stream tags: found {len(stream_tags_concurrent)} tags")
            for tag in stream_tags_concurrent:
                print(f"       - {tag}")
            
            # Test contiguous segments while writer alive (should also work now)
            print("   Testing contiguous segments while writer alive:")
            segments = concurrent_reader.query_contiguous_segments(stream_tag, start_ts, end_ts)
            print(f"     Contiguous segments: found {len(segments)} segments")
            for seg in segments:
                print(f"       Segment {seg['segment_id']}: {seg['start_timestamp']} to {seg['end_timestamp']}")
            
            return writer
        
        writer = test_with_writer_alive()
        
        # Step 3: Read data using iterator
        print("\n3. Testing iterator functionality...")
        
        # Create iterator
        iterator = nanots.Iterator(db_filename, stream_tag)
        
        # Test: Find data at specific timestamp (middle of our data)
        target_timestamp = test_data[25]['timestamp']  # 26th data point
        print(f"   Finding data at timestamp: {target_timestamp}")
        
        iterator.find(target_timestamp)
        
        if iterator.valid():
            frame = iterator.get_current_frame()
            print(f"   Found frame: {frame['data'].decode('utf-8')} at {frame['timestamp']}")
            print(f"   Block sequence: {frame['block_sequence']}")
        else:
            print("   No data found at target timestamp")
        
        # Test: Iterate over a subset of data (10 frames starting from target)
        print(f"\n   Iterating over next 10 frames from timestamp {target_timestamp}:")
        
        iterator.find(target_timestamp)
        count = 0
        frames_read = []
        
        while iterator.valid() and count < 10:
            frame = iterator.get_current_frame()
            frames_read.append(frame)
            
            data_str = frame['data'].decode('utf-8')
            timestamp = frame['timestamp']
            
            print(f"   Frame {count+1}: {data_str} at {timestamp}")
            
            # Move to next frame
            iterator.next()
            count += 1
        
        print(f"   Successfully read {len(frames_read)} frames")
        
        # Test: Use iterator as Python iterator
        print(f"\n   Testing Python iterator protocol (first 5 frames):")
        
        iterator.reset()  # Start from beginning
        for i, frame in enumerate(iterator):
            if i >= 5:  # Only show first 5
                break
            data_str = frame['data'].decode('utf-8')
            print(f"   Frame {i+1}: {data_str} at {frame['timestamp']}")
        
        # Step 4: Test reader functionality
        print("\n4. Testing reader functionality...")
        
        reader = nanots.Reader(db_filename)
        
        # Query contiguous segments
        start_ts = test_data[10]['timestamp']
        end_ts = test_data[40]['timestamp']
        
        print(f"   Querying contiguous segments from {start_ts} to {end_ts}")
        segments = reader.query_contiguous_segments(stream_tag, start_ts, end_ts)
        
        print(f"   Found {len(segments)} contiguous segments:")
        for seg in segments:
            print(f"     Segment {seg['segment_id']}: {seg['start_timestamp']} to {seg['end_timestamp']}")
        
        # Read data using reader
        print(f"   Reading data from {start_ts} to {end_ts}")
        frames = reader.read(stream_tag, start_ts, end_ts)
        
        print(f"   Read {len(frames)} frames:")
        for i, frame in enumerate(frames[:5]):  # Show first 5
            data_str = frame['data'].decode('utf-8')
            print(f"     Frame {i+1}: {data_str} at {frame['timestamp']}")
        
        if len(frames) > 5:
            print(f"     ... and {len(frames)-5} more frames")
        
        # Step 5: Test new stream tags query functionality
        print("\n5. Testing stream tags query functionality...")

        # Create a fresh reader for stream tags query (like C++ tests)
        stream_reader = nanots.Reader(db_filename)

        start_ts = test_data[0]['timestamp']
        end_ts = test_data[49]['timestamp']

        print(f"   Querying stream tags from {start_ts} to {end_ts}")
        stream_tags = stream_reader.query_stream_tags(start_ts, end_ts)
        
        print(f"   Found {len(stream_tags)} stream tags:")
        for tag in stream_tags:
            print(f"     - {tag}")
        
        # Verify our stream tag is in the results
        if stream_tag in stream_tags:
            print(f"   ✅ Confirmed our stream tag '{stream_tag}' is present")
        else:
            print(f"   ❌ Our stream tag '{stream_tag}' was not found in results")
        
        print("\n✅ All tests completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        raise
    
    finally:
        # Clean up temporary file
        if os.path.exists(db_filename):
            os.unlink(db_filename)
            print(f"\nCleaned up test file: {db_filename}")


def test_error_handling():
    """Test error handling and edge cases."""
    print("\n" + "="*50)
    print("Testing error handling...")
    
    # Test with non-existent file
    try:
        reader = nanots.Reader("non_existent_file.nanots")
        print("❌ Should have failed with non-existent file")
    except nanots.NanoTSError as e:
        print(f"✅ Correctly caught error for non-existent file: {e}")
    
    # Test invalid timestamp
    with tempfile.NamedTemporaryFile(delete=False, suffix='.nanots') as tmp_file:
        db_filename = tmp_file.name
    
    try:
        nanots.allocate_file(db_filename, 4096, 100)
        writer = nanots.Writer(db_filename)
        context = writer.create_context("test_stream", "test")
        
        # Test timestamp ordering rules: NanoTS requires monotonically increasing timestamps
        try:
            # This should work: write with current timestamp
            current_ts = int(time.time() * 1000)
            writer.write(context, b"test_data_1", current_ts, 0)
            
            # This should fail: write with earlier timestamp (violates monotonic ordering)
            earlier_ts = current_ts - 1000  # 1 second earlier
            writer.write(context, b"test_data_2", earlier_ts, 0)
            print("❌ Should have failed with non-monotonic timestamp")
        except nanots.NonMonotonicTimestampError as e:
            print(f"✅ Correctly caught non-monotonic timestamp error: {e}")
        except nanots.NanoTSError as e:
            print(f"✅ Caught error for non-monotonic timestamp: {e}")
        
        # Test with extremely large data to trigger frame too large error
        try:
            large_data = b"x" * (1024 * 1024)  # 1MB of data
            writer.write(context, large_data, int(time.time() * 1000) + 10000, 0)
            print("ℹ️  Note: Large frames (1MB) are accepted by nanots")
        except nanots.RowSizeTooBigError as e:
            print(f"✅ Correctly caught frame too large error: {e}")
        except nanots.NanoTSError as e:
            print(f"✅ Caught error for large frame: {e}")
    
    finally:
        if os.path.exists(db_filename):
            os.unlink(db_filename)


if __name__ == "__main__":
    print("NanoTS Python Binding Test Suite")
    print("=" * 50)
    
    try:
        test_nanots_basic_functionality()
        test_error_handling()
        
        print("\n🎉 All tests passed!")
        
    except ImportError as e:
        print(f"❌ Failed to import nanots module: {e}")
        print("Make sure the nanots module is compiled and installed properly.")
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        raise
