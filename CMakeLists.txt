cmake_minimum_required(VERSION 3.25)
project(nanots VERSION 0.0.1)

set(CMAKE_CXX_STANDARD 17)

include(settings.cmake)

file(GLOB NANOTS_HEADERS
    ${CMAKE_CURRENT_SOURCE_DIR}/*.h
)

file(GLOB NANOTS_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/*.c
)

add_library(
    nanots STATIC
    ${NANOTS_HEADERS}
    ${NANOTS_SOURCES}
)

target_include_directories(
    nanots PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

add_subdirectory(ut)
