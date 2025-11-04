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
    add_compile_definitions(IS_WINDOWS)
    add_compile_options(/W4 /MP /permissive- /Zc:preprocessor /wd4100)

    # Debug-specific flags (use generator expressions for multi-config generators like Visual Studio)
    add_compile_options(
        $<$<CONFIG:Debug>:/Od>                # disable optimizations
        $<$<CONFIG:Debug>:/Zi>                # debug info (PDB)
        $<$<CONFIG:Debug>:/RTC1>              # runtime checks (stack, uninit, etc.)
        $<$<CONFIG:Debug>:/Gy>                # function-level linking
        $<$<CONFIG:Debug>:/Zf>                # force inline debug info
    )
    add_link_options(
        $<$<CONFIG:Debug>:/DEBUG>
        $<$<CONFIG:Debug>:/INCREMENTAL>
    )

    # Release-specific flags
    add_compile_options(
        $<$<CONFIG:Release>:/O2>              # full optimization
        $<$<CONFIG:Release>:/Oi>              # intrinsic functions
        $<$<CONFIG:Release>:/GL>              # whole program optimization
        $<$<CONFIG:Release>:/Gy>              # function-level linking
    )
    add_link_options(
        $<$<CONFIG:Release>:/LTCG>            # link-time code generation
        $<$<CONFIG:Release>:/INCREMENTAL:NO>  # deterministic builds
    )
endif()
