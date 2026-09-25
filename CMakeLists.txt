cmake_minimum_required(VERSION 3.28)
project(test1 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 26)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_compile_options(-Wall -Wextra -Wpedantic)

add_executable(test1 test1.cpp)

add_executable(test_test1 test_test1.cpp)
add_executable(test_private_test1 test_private_test1.cpp)

enable_testing()
add_test(NAME unit_tests COMMAND test_test1)
add_test(NAME private_tests COMMAND test_private_test1)
add_test(NAME sample_feed COMMAND test1 ${CMAKE_SOURCE_DIR}/market_inputs.csv)
