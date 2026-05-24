// Demonstrates growable storage mode.
//
// In growable mode the file starts at just the 64KB header and is extended
// on demand using a BoltDB-style doubling strategy. We print the file size
// after each batch of writes so you can see it grow.

use nanots_rs::Writer;
use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = "growable_example.nts";

    // Use 64KB blocks so the file size jumps are easy to see.
    let block_size: u32 = 64 * 1024;

    // max_blocks = 0 means "grow until the disk is full." Pass a positive
    // value to bound the maximum file size.
    Writer::allocate_growable_file(path, block_size, 0)?;

    let print_size = |label: &str| {
        let sz = fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        println!("  file size {:<18}: {:>6} bytes", label, sz);
    };

    println!("=== Growable mode ===");
    print_size("after allocate");

    // Inner scope so writer and context drop (flushing sqlite metadata)
    // before we try to delete the files.
    {
        let writer = Writer::new(path, false)?;
        let ctx = writer.create_context("samples", "growable demo")?;

        // Each frame is half a block, so most writes force a new block,
        // which means most writes trigger a growth event during the
        // doubling phase.
        let payload: Vec<u8> = (0..(block_size / 2) as usize).map(|i| i as u8).collect();

        for i in 1..=8i64 {
            writer.write(&ctx, &payload, i * 1000, 0)?;
            print_size(&format!("after write {}", i));
        }
    }

    println!("\nFile grew on demand — no preallocation, no surprises.");

    let db = path.replace(".nts", ".db");
    fs::remove_file(path).ok();
    fs::remove_file(&db).ok();
    fs::remove_file(format!("{}-shm", db)).ok();
    fs::remove_file(format!("{}-wal", db)).ok();

    Ok(())
}
