include(FetchContent)
set(BENCHMARK_ENABLE_TESTING OFF)

FetchContent_Declare(benchmark URL https://github.com/google/benchmark/archive/refs/tags/v1.9.4.tar.gz)
FetchContent_MakeAvailable(benchmark)

# Aeron's C++ broadcast buffer (concurrent/broadcast/Broadcast{Transmitter,Receiver}.h) is header-only, so we only
# fetch the sources and never build Aeron. 1.49.x is the last release that ships the pure C++ client.
FetchContent_Declare(aeron
    URL https://github.com/aeron-io/aeron/archive/refs/tags/1.49.3.tar.gz
    URL_HASH SHA256=c98f0698bbb449628debd11661aa0fe66708c7df42db552fe05325647eb6540b
    SOURCE_SUBDIR headers-only)
FetchContent_MakeAvailable(aeron)

add_executable(Benchmarks_run throughput.cpp)
target_link_libraries(Benchmarks_run PRIVATE multiqueues multiqueues_warnings benchmark::benchmark_main)
target_include_directories(Benchmarks_run SYSTEM PRIVATE ${aeron_SOURCE_DIR}/aeron-client/src/main/cpp)
