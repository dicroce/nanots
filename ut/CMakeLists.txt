cmake_minimum_required(VERSION 3.25)
project(nanots_ut VERSION 0.0.1)

add_executable(
    nanots_ut
    include/framework.h
    source/framework.cpp
    include/test_nanots.h
    source/test_nanots.cpp
    include/test_nanots_c_api.h
    source/test_nanots_c_api.cpp
)

target_include_directories(
    nanots_ut PUBLIC 
    include
    ../
)

target_link_libraries(
    nanots_ut LINK_PUBLIC 
    nanots
    platform::platform
)
