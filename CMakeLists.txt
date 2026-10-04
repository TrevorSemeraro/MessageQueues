cmake_minimum_required(VERSION 4.3)
project(queues LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)

add_library(multiqueues INTERFACE)
target_include_directories(multiqueues INTERFACE include)
target_link_libraries(multiqueues INTERFACE Threads::Threads)

if(PROJECT_IS_TOP_LEVEL)
    # Applied to our own test/benchmark targets only, so fetched dependencies don't break the build.
    add_library(multiqueues_warnings INTERFACE)
    target_compile_options(multiqueues_warnings INTERFACE -Wall -Wextra -Wpedantic -Wno-interference-size -Werror)

    enable_testing()
    add_subdirectory(test)
    add_subdirectory(perf)
endif()
