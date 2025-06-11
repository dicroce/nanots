# setup.py
from setuptools import setup, Extension
import sys

# List ALL the source files that make up your extension.
all_source_files = [
    'nanots.pyx',                           # The Cython file in the root directory
    '../../amalgamated_src/nanots.cpp',     # Your C++ source file from amalgamated_src
    '../../amalgamated_src/sqlite3.c'       # The SQLite source file from amalgamated_src
]

extensions = [
    Extension(
        # The final compiled module name will be "nanots"
        "nanots",

        # The list of all source files to compile
        sources=all_source_files,

        # Tell the compiler to look for header files (.h) in the 'amalgamated_src' directory
        include_dirs=['../../amalgamated_src'],

        # Pass the C++17 flag based on platform
        extra_compile_args=['/std:c++17'] if sys.platform == 'win32' else ['-std=c++17'],

        # Tell setuptools to use the C++ compiler and linker
        language="c++",
    )
]

setup(
    name="nanots",
    version="0.1.0",
    description="Your project description here",
    ext_modules=extensions
)