#!/usr/bin/env python3

import shutil
import os
import sys
import argparse

def copy_tree_to_existing(src, dst, ignore_patterns=None):
    if ignore_patterns is None:
        ignore_patterns = ['.git']
    
    for root, dirs, files in os.walk(src):
        # Remove ignored directories from dirs list to prevent walking into them
        dirs[:] = [d for d in dirs if d not in ignore_patterns]
        
        # Create corresponding directory structure
        rel_path = os.path.relpath(root, src)
        dst_dir = os.path.join(dst, rel_path) if rel_path != '.' else dst
        os.makedirs(dst_dir, exist_ok=True)
        
        # Copy files
        for file in files:
            src_file = os.path.join(root, file)
            dst_file = os.path.join(dst_dir, file)
            shutil.copy2(src_file, dst_file)

def main():
    parser = argparse.ArgumentParser(description='Recursively copy directory tree while ignoring .git directories')
    parser.add_argument('source', help='Source directory to copy from')
    parser.add_argument('destination', help='Destination directory to copy to')
    parser.add_argument('--ignore', nargs='*', default=['.git'], 
                       help='Patterns to ignore (default: .git)')
    
    args = parser.parse_args()
    
    # Check if source directory exists
    if not os.path.exists(args.source):
        print(f"Error: Source directory '{args.source}' does not exist", file=sys.stderr)
        sys.exit(1)
    
    if not os.path.isdir(args.source):
        print(f"Error: Source '{args.source}' is not a directory", file=sys.stderr)
        sys.exit(1)
    
    try:
        print(f"Copying from '{args.source}' to '{args.destination}'...")
        print(f"Ignoring: {', '.join(args.ignore)}")
        copy_tree_to_existing(args.source, args.destination, args.ignore)
        print("Copy completed successfully!")
    except Exception as e:
        print(f"Error during copy: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
