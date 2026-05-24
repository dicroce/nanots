"""Demonstrates growable storage mode.

In growable mode the file starts at just the 64KB header and is extended
on demand using a BoltDB-style doubling strategy. This script prints the
file size after each write so you can watch the file grow.
"""

import os
import tempfile
import nanots


def growable_example():
    with tempfile.NamedTemporaryFile(delete=False, suffix='.nts') as tmp:
        db_file = tmp.name

    try:
        block_size = 64 * 1024  # 64KB blocks so size jumps are easy to see

        # max_blocks=0 means "grow until the disk is full." Pass a positive
        # value to bound the maximum file size.
        nanots.allocate_growable_file(db_file, block_size, 0)

        print("=== Growable mode ===")
        print(f"  file size after allocate: {os.path.getsize(db_file):6d} bytes")

        writer = nanots.Writer(db_file, auto_reclaim=False)
        ctx = writer.create_context("samples", "growable demo")

        # Each frame is half a block, so most writes force a new block, which
        # means most writes trigger a growth event during the doubling phase.
        payload = bytes(i & 0xff for i in range(block_size // 2))

        for i in range(1, 9):
            writer.write(ctx, payload, i * 1000, 0)
            print(f"  file size after write {i}: {os.path.getsize(db_file):6d} bytes")

        # Drop the writer/ctx explicitly so their destructors run (flushing
        # sqlite metadata) before we try to delete the files in `finally`.
        del ctx
        del writer

        print("\nFile grew on demand — no preallocation, no surprises.")

    finally:
        # Clean up the .nts file and its sqlite sidecar.
        for path in (db_file, db_file.replace('.nts', '.db'),
                     db_file.replace('.nts', '.db') + '-shm',
                     db_file.replace('.nts', '.db') + '-wal'):
            try:
                os.unlink(path)
            except OSError:
                pass


if __name__ == "__main__":
    growable_example()
