// Demonstrates growable storage mode.
//
// In growable mode the file starts at just the 64KB header and is extended
// on demand using a BoltDB-style doubling strategy. We print the file size
// after each batch of writes so you can see it grow.

package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/nanots/nanots-go"
)

func main() {
	tmpDir, err := os.MkdirTemp("", "nanots-growable-")
	if err != nil {
		log.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "growable.nts")

	// Use 64KB blocks so the file size jumps are easy to see.
	const blockSize = uint32(64 * 1024)

	// max_blocks=0 means "grow until the disk is full." Pass a positive
	// value to bound the maximum file size.
	if err := nanots.AllocateGrowableFile(dbPath, blockSize, 0); err != nil {
		log.Fatalf("AllocateGrowableFile: %v", err)
	}

	printSize := func(label string) {
		st, _ := os.Stat(dbPath)
		fmt.Printf("  file size %s: %6d bytes\n", label, st.Size())
	}

	fmt.Println("=== Growable mode ===")
	printSize("after allocate")

	writer, err := nanots.NewWriter(dbPath, false)
	if err != nil {
		log.Fatalf("NewWriter: %v", err)
	}
	defer writer.Close()

	ctx, err := writer.CreateWriteContext("samples", "growable demo")
	if err != nil {
		log.Fatalf("CreateWriteContext: %v", err)
	}
	defer ctx.Close()

	// Each frame is half a block, so most writes force a new block, which
	// means most writes trigger a growth event during the doubling phase.
	payload := make([]byte, blockSize/2)
	for i := 0; i < len(payload); i++ {
		payload[i] = byte(i)
	}

	for i := 1; i <= 8; i++ {
		if err := writer.Write(ctx, payload, int64(i*1000), 0); err != nil {
			log.Fatalf("Write %d: %v", i, err)
		}
		printSize(fmt.Sprintf("after write %d", i))
	}

	fmt.Println("\nFile grew on demand — no preallocation, no surprises.")
}
