from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext
from Cython.Build import cythonize
import sys
import os

class MixedBuildExt(build_ext):
    def run(self):
        # Initialize the compiler first
        super().run()
    
    def build_extension(self, ext):
        # Handle sqlite3.c separately for each extension
        if '../../amalgamated_src/sqlite3.c' in ext.sources:
            # Compile sqlite3.c as C without C++ flags
            sqlite_source = '../../amalgamated_src/sqlite3.c'
            
            # Remove sqlite3.c from the main sources
            ext.sources = [src for src in ext.sources if not src.endswith('sqlite3.c')]
            
            # Compile the C file separately
            # Ensure the output directory exists
            os.makedirs(self.build_temp, exist_ok=True)
            sqlite_objects = self.compiler.compile(
                [sqlite_source],
                output_dir=self.build_temp,
                include_dirs=ext.include_dirs,
                debug=self.debug,
                extra_preargs=[],  # No C++ flags for C file
                depends=ext.depends
            )
            
            # Add the compiled object to extra_objects
            if not hasattr(ext, 'extra_objects'):
                ext.extra_objects = []
            ext.extra_objects.extend(sqlite_objects)
        
        # Build the rest of the extension normally (handles .pyx and .cpp with C++ flags)
        super().build_extension(ext)

# Define the extension
extensions = [
    Extension(
        "nanots",
        sources=[
            'nanots.pyx',
            '../../amalgamated_src/nanots.cpp',
            '../../amalgamated_src/sqlite3.c'
        ],
        include_dirs=['../../amalgamated_src'],
        # C++17 flags for C++ files only
        extra_compile_args=['/std:c++17'] if sys.platform == 'win32' else ['-std=c++17'],
        language="c++",
    )
]

# Cythonize the extensions
extensions = cythonize(extensions, language_level=3)

setup(
    name="nanots",
    version="0.1.0", 
    description="Python bindings for nanots embedded time series database",
    ext_modules=extensions,
    cmdclass={'build_ext': MixedBuildExt},
)