include(FetchContent)
FetchContent_Declare(googletest URL https://github.com/google/googletest/archive/refs/tags/v1.17.0.tar.gz)
FetchContent_MakeAvailable(googletest)

add_executable(
        Google_Tests_run
        spsc_queue_tests.cpp
        spmc_queue_tests.cpp
)
target_link_libraries(Google_Tests_run PRIVATE multiqueues multiqueues_warnings GTest::gtest_main)

# The tests always run under ThreadSanitizer and UBSan; any UB report fails the run.
set(SANITIZE -fsanitize=thread,undefined -fno-sanitize-recover=undefined)
target_compile_options(Google_Tests_run PRIVATE ${SANITIZE} -fno-omit-frame-pointer)
target_link_options(Google_Tests_run PRIVATE ${SANITIZE})

include(GoogleTest)
gtest_discover_tests(Google_Tests_run)
