include(TestBigEndian)
set(CMAKE_CXX_STANDARD 17)

if(IS_BIG_ENDIAN)
    add_compile_definitions(IS_BIG_ENDIAN)
else()
    add_compile_definitions(IS_LITTLE_ENDIAN)
endif()

if(NOT CMAKE_BUILD_TYPE)
    set (CMAKE_BUILD_TYPE Debug)
endif()

# Remove runtime-check flag from all configs to avoid /O2 vs /RTC1 conflicts
string(REPLACE "/RTC1" "" CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG}")
string(REPLACE "/RTC1" "" CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG}")
string(REPLACE "/RTC1" "" CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE}")
string(REPLACE "/RTC1" "" CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")
string(REPLACE "/RTC1" "" CMAKE_C_FLAGS_RELWITHDEBINFO "${CMAKE_C_FLAGS_RELWITHDEBINFO}")
string(REPLACE "/RTC1" "" CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO}")

if(WIN32)
    # Remove the default /D _DEBUG flag that CMake injects
    string(REPLACE "/D _DEBUG" "" CMAKE_C_FLAGS_DEBUG   "${CMAKE_C_FLAGS_DEBUG}")
    string(REPLACE "/D _DEBUG" "" CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG}")

    add_compile_definitions(
        $<$<CONFIG:Debug>:NDEBUG>
    )
endif()

# Apply build flags per platform and configuration
if(CMAKE_SYSTEM_NAME MATCHES "Linux")
    add_compile_options(-Wall -Wextra -Wno-unused-parameter)

    if(CMAKE_BUILD_TYPE STREQUAL "Debug")
        message(STATUS "Applying debug build flags for Linux")

        add_compile_options(
            -g3
            -ggdb3
            -O0
            -fsanitize=address
            -fno-omit-frame-pointer
            -fasynchronous-unwind-tables
        )
        add_link_options(
            -g3
            -ggdb3
            -fsanitize=address
            -fno-omit-frame-pointer
            -fasynchronous-unwind-tables
        )
    elseif(CMAKE_BUILD_TYPE STREQUAL "Release")
        message(STATUS "Applying release build flags for Linux")

        add_compile_options(
            -O3
            -march=native
            -ffast-math
            -fno-math-errno
            -fno-omit-frame-pointer
        )
        add_link_options(
            -O3
            -march=native
        )
    endif()

elseif(CMAKE_SYSTEM_NAME MATCHES "Windows")
    add_compile_options(/W4 /MP /permissive- /Zc:preprocessor)

    if(CMAKE_BUILD_TYPE STREQUAL "Debug")
        message(STATUS "Applying debug build flags for Windows")

        add_compile_options(
            /Od                # disable optimizations
            /Zi                # debug info (PDB)
            /RTC1              # runtime checks (stack, uninit, etc.)
            /Gy                # function-level linking
            /Zf                # force inline debug info
            /wd4100
        )
        add_link_options(
            /DEBUG
            /INCREMENTAL
        )

    elseif(CMAKE_BUILD_TYPE STREQUAL "Release")
        message(STATUS "Applying release build flags for Windows")

        add_compile_options(
            /O2                # full optimization
            /Oi                # intrinsic functions
            /GL                # whole program optimization
            /Gy                # function-level linking
            /wd4100
        )
        add_link_options(
            /LTCG              # link-time code generation
            /INCREMENTAL:NO    # deterministic builds
        )
    endif()
endif()
