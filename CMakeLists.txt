cmake_minimum_required(VERSION 3.25)
project(nanots VERSION 0.0.1)

set(CMAKE_CXX_STANDARD 17)

include(settings.cmake)
include(dependencies.cmake)

file(GLOB NANOTS_HEADERS
    ${CMAKE_CURRENT_SOURCE_DIR}/*.h
)

file(GLOB NANOTS_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/*.c
)

# Build static library
add_library(
    nanots_static STATIC
    ${NANOTS_HEADERS}
    ${NANOTS_SOURCES}
)

target_include_directories(
    nanots_static PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# Build shared library
add_library(
    nanots SHARED
    ${NANOTS_HEADERS}
    ${NANOTS_SOURCES}
)

target_include_directories(
    nanots PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# Set output names to avoid conflicts
set_target_properties(nanots_static PROPERTIES
    OUTPUT_NAME "nanots_static"
)

set_target_properties(nanots PROPERTIES
    OUTPUT_NAME "nanots"
)

# Define NANOTS_BUILDING_DLL when building the shared library
target_compile_definitions(nanots PRIVATE NANOTS_BUILDING_DLL)

# Make SQLite symbols internal/private on all platforms
# This ensures SQLITE_API doesn't try to export symbols
target_compile_definitions(nanots_static PRIVATE SQLITE_API=)
target_compile_definitions(nanots PRIVATE SQLITE_API=)

# Hide symbols by default on Linux/Unix and make SQLite symbols private
if(UNIX)
    # Set default visibility to hidden for both libraries
    target_compile_options(nanots_static PRIVATE -fvisibility=hidden)
    target_compile_options(nanots PRIVATE -fvisibility=hidden)

    # Use linker version script for shared library to control exported symbols
    set_target_properties(nanots PROPERTIES
        LINK_FLAGS "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/nanots.version"
    )
endif()

# Link platform dependencies to both libraries
target_link_libraries(nanots_static PUBLIC platform::platform)
target_link_libraries(nanots PUBLIC platform::platform)

add_subdirectory(ut)
