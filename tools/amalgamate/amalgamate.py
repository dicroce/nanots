import os
import re
from pathlib import Path

def remove_include_load(path, includes_to_remove_list):
    """
    Load a file and remove specified include lines.
    
    Args:
        path: Path to the source file
        includes_to_remove_list: List of include patterns to remove (e.g., ['#include "header.h"'])
    
    Returns:
        String content with specified includes removed
    """
    with open(path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
    
    filtered_lines = []
    for line in lines:
        stripped_line = line.strip()
        should_remove = False
        
        for include_pattern in includes_to_remove_list:
            if stripped_line == include_pattern.strip():
                should_remove = True
                break
        
        if not should_remove:
            filtered_lines.append(line)
    
    return ''.join(filtered_lines)

def write_lines_to_file(filename, lines_list):
    """
    Write a list of strings to a file, joining them with newlines.
    
    Args:
        filename: Path/name of the file to write to
        lines_list: List of strings to write to the file
    """
    with open(filename, 'w', encoding='utf-8') as file:
        file.write('\n'.join(lines_list))

if __name__ == "__main__":
    # Create amalgamated_src directory if it doesn't exist
    os.makedirs("amalgamated_src", exist_ok=True)
    
    print("Creating amalgamation...")
    
    # Copy sqlite3 files as-is (they're already amalgamated)
    print("Copying SQLite files...")
    
    # Just copy sqlite3.h and sqlite3.c directly
    with open("sqlite3.h", 'r') as f:
        sqlite3_h = f.read()
    with open("sqlite3.c", 'r') as f:
        sqlite3_c = f.read()
    
    with open("amalgamated_src/sqlite3.h", 'w') as f:
        f.write(sqlite3_h)
    with open("amalgamated_src/sqlite3.c", 'w') as f:
        f.write(sqlite3_c)
    
    # Create amalgamated header for your C++ code
    utils_h_content = remove_include_load("utils.h", ["#include \"sqlite3.h\""])
    nanots_h_content = remove_include_load("nanots.h", ["#include \"utils.h\""])

    header_parts = [
        "// Amalgamated header file",
        "// Generated automatically - do not edit",
        "",
        "#ifndef NANOTS_AMALGAMATED_H",
        "#define NANOTS_AMALGAMATED_H",
        "",
        '#include "sqlite3.h"',
        "",
        utils_h_content,
        "",
        nanots_h_content,
        "",
        "#endif // NANOTS_AMALGAMATED_H"
    ]

    write_lines_to_file("amalgamated_src/nanots.h", header_parts)

    # Create amalgamated C++ source (without SQLite)
    utils_cpp_content = remove_include_load("utils.cpp", ["#include \"sqlite3.h\"", "#include \"utils.h\""])
    nanots_cpp_content = remove_include_load("nanots.cpp", ["#include \"nanots.h\""])

    source_parts = [
        "// Amalgamated C++ source file", 
        "// Generated automatically - do not edit",
        "",
        '#include "nanots.h"',
        "",
        "// Utils implementation", 
        utils_cpp_content,
        "",
        "// Nanots implementation",
        nanots_cpp_content
    ]

    write_lines_to_file("amalgamated_src/nanots.cpp", source_parts)
    
    print("Amalgamation complete!")
    print("Compile with: gcc -c sqlite3.c -o sqlite3.o && g++ main.cpp nanots.cpp sqlite3.o -o test")
    print("Or simply: gcc -c sqlite3.c && g++ main.cpp nanots.cpp sqlite3.o -o test")
