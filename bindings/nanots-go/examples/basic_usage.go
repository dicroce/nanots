package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/nanots/nanots-go"
)

func main() {
	// Create a temporary directory for the example
	tmpDir, err := os.MkdirTemp("", "nanots-example-")
	if err != nil {
		log.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "example.nts")
	fmt.Printf("Creating NanoTS database at: %s\n\n", dbPath)

	// Step 1: Allocate a new database file
	fmt.Println("=== Step 1: Allocating Database ===")
	blockSize := uint32(1024 * 1024) // 1MB blocks
	nBlocks := uint32(10)             // 10 blocks
	err = nanots.AllocateFile(dbPath, blockSize, nBlocks)
	if err != nil {
		log.Fatalf("Failed to allocate database file: %v", err)
	}
	fmt.Printf("Allocated database with %d blocks of %d bytes each\n\n", nBlocks, blockSize)

	// Step 2: Write data to multiple streams
	fmt.Println("=== Step 2: Writing Data ===")
	{
		writer, err := nanots.NewWriter(dbPath, false)
		if err != nil {
			log.Fatalf("Failed to create writer: %v", err)
		}
		defer writer.Close()

		// Create context for temperature sensor stream
		tempCtx, err := writer.CreateWriteContext("sensor/temperature", `{"unit": "celsius", "location": "room1"}`)
		if err != nil {
			log.Fatalf("Failed to create temperature context: %v", err)
		}
		defer tempCtx.Close()

		// Create context for humidity sensor stream
		humidCtx, err := writer.CreateWriteContext("sensor/humidity", `{"unit": "percent", "location": "room1"}`)
		if err != nil {
			log.Fatalf("Failed to create humidity context: %v", err)
		}
		defer humidCtx.Close()

		// Write temperature readings
		baseTime := time.Now().UnixMilli()
		temperatures := []float64{20.5, 21.0, 21.5, 22.0, 22.5}
		for i, temp := range temperatures {
			data := fmt.Sprintf("%.1f", temp)
			timestamp := baseTime + int64(i*1000) // 1 second apart
			err = writer.Write(tempCtx, []byte(data), timestamp, 0)
			if err != nil {
				log.Fatalf("Failed to write temperature: %v", err)
			}
			fmt.Printf("Wrote temperature: %s°C at timestamp %d\n", data, timestamp)
		}

		// Write humidity readings
		humidities := []float64{45.0, 46.5, 47.0, 48.5, 50.0}
		for i, humid := range humidities {
			data := fmt.Sprintf("%.1f", humid)
			timestamp := baseTime + int64(i*1000)
			err = writer.Write(humidCtx, []byte(data), timestamp, 0)
			if err != nil {
				log.Fatalf("Failed to write humidity: %v", err)
			}
			fmt.Printf("Wrote humidity: %s%% at timestamp %d\n", data, timestamp)
		}
		fmt.Println()
	}

	// Step 3: Read data using callback
	fmt.Println("=== Step 3: Reading Data with Callback ===")
	{
		reader, err := nanots.NewReader(dbPath)
		if err != nil {
			log.Fatalf("Failed to create reader: %v", err)
		}
		defer reader.Close()

		fmt.Println("Temperature readings:")
		err = reader.Read("sensor/temperature", 0, 9223372036854775807, func(frame nanots.Frame) error {
			fmt.Printf("  Timestamp: %d, Data: %s, Metadata: %s\n",
				frame.Timestamp, string(frame.Data), frame.Metadata)
			return nil
		})
		if err != nil {
			log.Fatalf("Failed to read temperature data: %v", err)
		}

		fmt.Println("\nHumidity readings:")
		err = reader.Read("sensor/humidity", 0, 9223372036854775807, func(frame nanots.Frame) error {
			fmt.Printf("  Timestamp: %d, Data: %s, Metadata: %s\n",
				frame.Timestamp, string(frame.Data), frame.Metadata)
			return nil
		})
		if err != nil {
			log.Fatalf("Failed to read humidity data: %v", err)
		}
		fmt.Println()
	}

	// Step 4: Query available streams
	fmt.Println("=== Step 4: Querying Stream Tags ===")
	{
		reader, err := nanots.NewReader(dbPath)
		if err != nil {
			log.Fatalf("Failed to create reader: %v", err)
		}
		defer reader.Close()

		tags, err := reader.QueryStreamTags(0, 9223372036854775807)
		if err != nil {
			log.Fatalf("Failed to query stream tags: %v", err)
		}

		fmt.Printf("Found %d streams:\n", len(tags))
		for _, tag := range tags {
			fmt.Printf("  - %s\n", tag)
		}
		fmt.Println()
	}

	// Step 5: Use iterator for sequential access
	fmt.Println("=== Step 5: Using Iterator ===")
	{
		iter, err := nanots.NewIterator(dbPath, "sensor/temperature")
		if err != nil {
			log.Fatalf("Failed to create iterator: %v", err)
		}
		defer iter.Close()

		err = iter.Reset()
		if err != nil {
			log.Fatalf("Failed to reset iterator: %v", err)
		}

		fmt.Println("Iterating through temperature readings:")
		count := 0
		for iter.Valid() {
			frame, err := iter.Current()
			if err != nil {
				log.Fatalf("Failed to get current frame: %v", err)
			}

			fmt.Printf("  [%d] Timestamp: %d, Data: %s, BlockSeq: %d\n",
				count, frame.Timestamp, string(frame.Data), frame.BlockSequence)

			err = iter.Next()
			if err != nil {
				break
			}
			count++
		}
		fmt.Printf("Total frames: %d\n\n", count)
	}

	// Step 6: Use iterator Find for timestamp-based seeking
	fmt.Println("=== Step 6: Finding Specific Timestamp ===")
	{
		iter, err := nanots.NewIterator(dbPath, "sensor/temperature")
		if err != nil {
			log.Fatalf("Failed to create iterator: %v", err)
		}
		defer iter.Close()

		// Get first timestamp
		err = iter.Reset()
		if err != nil {
			log.Fatalf("Failed to reset iterator: %v", err)
		}
		firstFrame, _ := iter.Current()
		searchTime := firstFrame.Timestamp + 2000 // Search for timestamp 2 seconds later

		err = iter.Find(searchTime)
		if err != nil {
			log.Fatalf("Failed to find timestamp: %v", err)
		}

		if iter.Valid() {
			frame, err := iter.Current()
			if err != nil {
				log.Fatalf("Failed to get current frame: %v", err)
			}
			fmt.Printf("Found frame at or after timestamp %d:\n", searchTime)
			fmt.Printf("  Timestamp: %d, Data: %s\n", frame.Timestamp, string(frame.Data))
		}
		fmt.Println()
	}

	// Step 7: Query contiguous segments
	fmt.Println("=== Step 7: Querying Contiguous Segments ===")
	{
		reader, err := nanots.NewReader(dbPath)
		if err != nil {
			log.Fatalf("Failed to create reader: %v", err)
		}
		defer reader.Close()

		segments, err := reader.QueryContiguousSegments("sensor/temperature", 0, 9223372036854775807)
		if err != nil {
			log.Fatalf("Failed to query segments: %v", err)
		}

		fmt.Printf("Found %d contiguous segment(s):\n", len(segments))
		for i, seg := range segments {
			fmt.Printf("  Segment %d: ID=%d, Start=%d, End=%d\n",
				i, seg.SegmentID, seg.StartTimestamp, seg.EndTimestamp)
		}
	}

	fmt.Println("\n=== Example Complete ===")
	fmt.Printf("Database file: %s\n", dbPath)
	stat, _ := os.Stat(dbPath)
	fmt.Printf("Database size: %d bytes\n", stat.Size())
}
