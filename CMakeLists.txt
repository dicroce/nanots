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
    WINDOWS_EXPORT_ALL_SYMBOLS ON
)

# Link platform dependencies to both libraries
target_link_libraries(nanots_static PUBLIC platform::platform)
target_link_libraries(nanots PUBLIC platform::platform)

add_subdirectory(ut)
