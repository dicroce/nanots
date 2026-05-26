package nanots

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestBasicWriteReadCycle(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.nts")

	// Allocate file
	err := AllocateFile(filePath, 1024*1024, 10)
	if err != nil {
		t.Fatalf("Failed to allocate file: %v", err)
	}

	// Write data
	{
		writer, err := NewWriter(filePath, false)
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}
		defer writer.Close()

		ctx, err := writer.CreateWriteContext("test_stream", "test metadata")
		if err != nil {
			t.Fatalf("Failed to create write context: %v", err)
		}
		defer ctx.Close()

		err = writer.Write(ctx, []byte("hello world"), 0, 1000)
		if err != nil {
			t.Fatalf("Failed to write first frame: %v", err)
		}

		err = writer.Write(ctx, []byte("goodbye world"), 1, 2000)
		if err != nil {
			t.Fatalf("Failed to write second frame: %v", err)
		}
	}

	// Read data back
	{
		reader, err := NewReader(filePath)
		if err != nil {
			t.Fatalf("Failed to create reader: %v", err)
		}
		defer reader.Close()

		type record struct {
			data      []byte
			flags     uint32
			timestamp int64
		}
		var records []record

		err = reader.Read("test_stream", 0, 9223372036854775807, func(frame Frame) error {
			records = append(records, record{
				data:      frame.Data,
				flags:     frame.Flags,
				timestamp: frame.Timestamp,
			})
			return nil
		})
		if err != nil {
			t.Fatalf("Failed to read data: %v", err)
		}

		if len(records) != 2 {
			t.Fatalf("Expected 2 records, got %d", len(records))
		}
		if string(records[0].data) != "hello world" {
			t.Errorf("Expected 'hello world', got '%s'", string(records[0].data))
		}
		if records[0].flags != 0 {
			t.Errorf("Expected flags 0, got %d", records[0].flags)
		}
		if records[0].timestamp != 1000 {
			t.Errorf("Expected timestamp 1000, got %d", records[0].timestamp)
		}
		if string(records[1].data) != "goodbye world" {
			t.Errorf("Expected 'goodbye world', got '%s'", string(records[1].data))
		}
		if records[1].flags != 1 {
			t.Errorf("Expected flags 1, got %d", records[1].flags)
		}
		if records[1].timestamp != 2000 {
			t.Errorf("Expected timestamp 2000, got %d", records[1].timestamp)
		}
	}
}

func TestIteratorInterface(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.nts")

	// Allocate and write test data
	err := AllocateFile(filePath, 1024*1024, 10)
	if err != nil {
		t.Fatalf("Failed to allocate file: %v", err)
	}

	{
		writer, err := NewWriter(filePath, false)
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}
		defer writer.Close()

		ctx, err := writer.CreateWriteContext("iter_test", "iterator test")
		if err != nil {
			t.Fatalf("Failed to create write context: %v", err)
		}
		defer ctx.Close()

		for i := 0; i < 5; i++ {
			data := fmt.Sprintf("item_%d", i)
			err = writer.Write(ctx, []byte(data), 0, int64(i*1000))
			if err != nil {
				t.Fatalf("Failed to write frame %d: %v", i, err)
			}
		}
	}

	// Test iterator
	{
		iter, err := NewIterator(filePath, "iter_test")
		if err != nil {
			t.Fatalf("Failed to create iterator: %v", err)
		}
		defer iter.Close()

		err = iter.Reset()
		if err != nil {
			t.Fatalf("Failed to reset iterator: %v", err)
		}

		count := 0
		for iter.Valid() {
			frame, err := iter.Current()
			if err != nil {
				t.Fatalf("Failed to get current frame: %v", err)
			}

			expectedData := fmt.Sprintf("item_%d", count)
			if string(frame.Data) != expectedData {
				t.Errorf("Expected data '%s', got '%s'", expectedData, string(frame.Data))
			}
			if frame.Timestamp != int64(count*1000) {
				t.Errorf("Expected timestamp %d, got %d", count*1000, frame.Timestamp)
			}

			err = iter.Next()
			if err != nil {
				// It's okay if Next returns an error at the end
				break
			}
			count++
		}

		if count != 5 {
			t.Errorf("Expected to iterate over 5 items, got %d", count)
		}
	}
}

func TestFindFunctionality(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.nts")

	// Allocate and write test data
	err := AllocateFile(filePath, 1024*1024, 10)
	if err != nil {
		t.Fatalf("Failed to allocate file: %v", err)
	}

	{
		writer, err := NewWriter(filePath, false)
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}
		defer writer.Close()

		ctx, err := writer.CreateWriteContext("find_test", "find test")
		if err != nil {
			t.Fatalf("Failed to create write context: %v", err)
		}
		defer ctx.Close()

		// Write data at specific timestamps
		timestamps := []int64{1000, 2000, 3000, 5000, 8000}
		for i, timestamp := range timestamps {
			data := fmt.Sprintf("data_%d", i)
			err = writer.Write(ctx, []byte(data), 0, timestamp)
			if err != nil {
				t.Fatalf("Failed to write frame %d: %v", i, err)
			}
		}
	}

	// Test find functionality
	{
		iter, err := NewIterator(filePath, "find_test")
		if err != nil {
			t.Fatalf("Failed to create iterator: %v", err)
		}
		defer iter.Close()

		// Find timestamp that exists
		err = iter.Find(3000, SecKeyUnset)
		if err != nil {
			t.Fatalf("Failed to find timestamp 3000: %v", err)
		}
		if !iter.Valid() {
			t.Fatal("Iterator should be valid after find")
		}
		frame, err := iter.Current()
		if err != nil {
			t.Fatalf("Failed to get current frame: %v", err)
		}
		if frame.Timestamp != 3000 {
			t.Errorf("Expected timestamp 3000, got %d", frame.Timestamp)
		}
		if string(frame.Data) != "data_2" {
			t.Errorf("Expected data 'data_2', got '%s'", string(frame.Data))
		}

		// Find timestamp between existing ones (should find next)
		err = iter.Find(4000, SecKeyUnset)
		if err != nil {
			t.Fatalf("Failed to find timestamp 4000: %v", err)
		}
		if !iter.Valid() {
			t.Fatal("Iterator should be valid after find")
		}
		frame, err = iter.Current()
		if err != nil {
			t.Fatalf("Failed to get current frame: %v", err)
		}
		if frame.Timestamp != 5000 {
			t.Errorf("Expected timestamp 5000, got %d", frame.Timestamp)
		}
		if string(frame.Data) != "data_3" {
			t.Errorf("Expected data 'data_3', got '%s'", string(frame.Data))
		}

		// Find timestamp before any data
		err = iter.Find(500, SecKeyUnset)
		if err != nil {
			t.Fatalf("Failed to find timestamp 500: %v", err)
		}
		if !iter.Valid() {
			t.Fatal("Iterator should be valid after find")
		}
		frame, err = iter.Current()
		if err != nil {
			t.Fatalf("Failed to get current frame: %v", err)
		}
		if frame.Timestamp != 1000 {
			t.Errorf("Expected timestamp 1000, got %d", frame.Timestamp)
		}
		if string(frame.Data) != "data_0" {
			t.Errorf("Expected data 'data_0', got '%s'", string(frame.Data))
		}
	}
}

func TestContiguousSegments(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.nts")

	// Allocate and write test data
	err := AllocateFile(filePath, 1024*1024, 10)
	if err != nil {
		t.Fatalf("Failed to allocate file: %v", err)
	}

	{
		writer, err := NewWriter(filePath, false)
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}
		defer writer.Close()

		ctx, err := writer.CreateWriteContext("segment_test", "segment test")
		if err != nil {
			t.Fatalf("Failed to create write context: %v", err)
		}
		defer ctx.Close()

		err = writer.Write(ctx, []byte("test data"), 0, 1000)
		if err != nil {
			t.Fatalf("Failed to write data: %v", err)
		}
	}

	// Query segments
	{
		reader, err := NewReader(filePath)
		if err != nil {
			t.Fatalf("Failed to create reader: %v", err)
		}
		defer reader.Close()

		segments, err := reader.QueryContiguousSegments("segment_test", 0, 9223372036854775807)
		if err != nil {
			t.Fatalf("Failed to query segments: %v", err)
		}

		// Should have at least one segment
		if len(segments) == 0 {
			t.Fatal("Expected at least one segment")
		}
		if segments[0].StartTimestamp > 1000 {
			t.Errorf("Expected start timestamp <= 1000, got %d", segments[0].StartTimestamp)
		}
		if segments[0].EndTimestamp < 1000 {
			t.Errorf("Expected end timestamp >= 1000, got %d", segments[0].EndTimestamp)
		}
	}
}

func TestErrorHandling(t *testing.T) {
	// Test opening non-existent file
	_, err := NewReader("/non/existent/file.nts")
	if err == nil {
		t.Fatal("Expected error when opening non-existent file")
	}
	if ntsErr, ok := err.(*Error); ok {
		if ntsErr.Code != ErrCantOpen {
			t.Errorf("Expected ErrCantOpen, got %v", ntsErr.Code)
		}
	}

	// Test iterator on non-existent file
	_, err = NewIterator("/non/existent/file.nts", "test")
	if err == nil {
		t.Fatal("Expected error when creating iterator for non-existent file")
	}
}

func TestMultipleStreams(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.nts")

	// Allocate file
	err := AllocateFile(filePath, 1024*1024, 20)
	if err != nil {
		t.Fatalf("Failed to allocate file: %v", err)
	}

	// Write to multiple streams
	{
		writer, err := NewWriter(filePath, false)
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}
		defer writer.Close()

		ctx1, err := writer.CreateWriteContext("stream1", "first stream")
		if err != nil {
			t.Fatalf("Failed to create context for stream1: %v", err)
		}
		defer ctx1.Close()

		ctx2, err := writer.CreateWriteContext("stream2", "second stream")
		if err != nil {
			t.Fatalf("Failed to create context for stream2: %v", err)
		}
		defer ctx2.Close()

		err = writer.Write(ctx1, []byte("stream1_data1"), 0, 1000)
		if err != nil {
			t.Fatalf("Failed to write to stream1: %v", err)
		}

		err = writer.Write(ctx2, []byte("stream2_data1"), 0, 1500)
		if err != nil {
			t.Fatalf("Failed to write to stream2: %v", err)
		}

		err = writer.Write(ctx1, []byte("stream1_data2"), 0, 2000)
		if err != nil {
			t.Fatalf("Failed to write to stream1: %v", err)
		}

		err = writer.Write(ctx2, []byte("stream2_data2"), 0, 2500)
		if err != nil {
			t.Fatalf("Failed to write to stream2: %v", err)
		}
	}

	// Read from each stream separately
	{
		reader, err := NewReader(filePath)
		if err != nil {
			t.Fatalf("Failed to create reader: %v", err)
		}
		defer reader.Close()

		// Read stream1
		type streamData struct {
			data      []byte
			timestamp int64
		}
		var stream1Data []streamData
		err = reader.Read("stream1", 0, 9223372036854775807, func(frame Frame) error {
			stream1Data = append(stream1Data, streamData{
				data:      frame.Data,
				timestamp: frame.Timestamp,
			})
			return nil
		})
		if err != nil {
			t.Fatalf("Failed to read stream1: %v", err)
		}

		if len(stream1Data) != 2 {
			t.Fatalf("Expected 2 records from stream1, got %d", len(stream1Data))
		}
		if string(stream1Data[0].data) != "stream1_data1" {
			t.Errorf("Expected 'stream1_data1', got '%s'", string(stream1Data[0].data))
		}
		if stream1Data[0].timestamp != 1000 {
			t.Errorf("Expected timestamp 1000, got %d", stream1Data[0].timestamp)
		}
		if string(stream1Data[1].data) != "stream1_data2" {
			t.Errorf("Expected 'stream1_data2', got '%s'", string(stream1Data[1].data))
		}
		if stream1Data[1].timestamp != 2000 {
			t.Errorf("Expected timestamp 2000, got %d", stream1Data[1].timestamp)
		}

		// Read stream2
		var stream2Data []streamData
		err = reader.Read("stream2", 0, 9223372036854775807, func(frame Frame) error {
			stream2Data = append(stream2Data, streamData{
				data:      frame.Data,
				timestamp: frame.Timestamp,
			})
			return nil
		})
		if err != nil {
			t.Fatalf("Failed to read stream2: %v", err)
		}

		if len(stream2Data) != 2 {
			t.Fatalf("Expected 2 records from stream2, got %d", len(stream2Data))
		}
		if string(stream2Data[0].data) != "stream2_data1" {
			t.Errorf("Expected 'stream2_data1', got '%s'", string(stream2Data[0].data))
		}
		if stream2Data[0].timestamp != 1500 {
			t.Errorf("Expected timestamp 1500, got %d", stream2Data[0].timestamp)
		}
		if string(stream2Data[1].data) != "stream2_data2" {
			t.Errorf("Expected 'stream2_data2', got '%s'", string(stream2Data[1].data))
		}
		if stream2Data[1].timestamp != 2500 {
			t.Errorf("Expected timestamp 2500, got %d", stream2Data[1].timestamp)
		}
	}
}

func TestQueryStreamTags(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.nts")

	// Allocate file
	err := AllocateFile(filePath, 1024*1024, 20)
	if err != nil {
		t.Fatalf("Failed to allocate file: %v", err)
	}

	// Write to multiple streams
	{
		writer, err := NewWriter(filePath, false)
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}
		defer writer.Close()

		streams := []string{"stream_a", "stream_b", "stream_c"}
		for _, streamTag := range streams {
			ctx, err := writer.CreateWriteContext(streamTag, "test metadata")
			if err != nil {
				t.Fatalf("Failed to create context for %s: %v", streamTag, err)
			}
			err = writer.Write(ctx, []byte("data"), 0, 1000)
			if err != nil {
				t.Fatalf("Failed to write to %s: %v", streamTag, err)
			}
			ctx.Close()
		}
	}

	// Query stream tags
	{
		reader, err := NewReader(filePath)
		if err != nil {
			t.Fatalf("Failed to create reader: %v", err)
		}
		defer reader.Close()

		tags, err := reader.QueryStreamTags(0, 9223372036854775807)
		if err != nil {
			t.Fatalf("Failed to query stream tags: %v", err)
		}

		if len(tags) != 3 {
			t.Fatalf("Expected 3 stream tags, got %d", len(tags))
		}

		// Check that all expected tags are present
		tagMap := make(map[string]bool)
		for _, tag := range tags {
			tagMap[tag] = true
		}
		for _, expected := range []string{"stream_a", "stream_b", "stream_c"} {
			if !tagMap[expected] {
				t.Errorf("Expected to find stream tag '%s'", expected)
			}
		}
	}
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
