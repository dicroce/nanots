
# ---- Platform target (to abstract away OS-specific system libs) ----
if(NOT TARGET platform::platform)
    add_library(platform::platform INTERFACE IMPORTED GLOBAL)
    if(CMAKE_SYSTEM_NAME MATCHES "Linux")
        target_link_libraries(platform::platform INTERFACE
            pthread
            dl
        )
    endif()
endif()
