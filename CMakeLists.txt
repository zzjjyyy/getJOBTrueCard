cmake_minimum_required(VERSION 2.8.12)
project(SQL)

set(CMAKE_CXX_STANDARD 11)
# set(CMAKE_BUILD_TYPE "Debug")

include_directories(../giveDuckTopDownOptimizer/src/include)
link_directories(../giveDuckTopDownOptimizer/src)

add_executable(GetOptimal4JOB main.cpp)
target_link_libraries(GetOptimal4JOB duckdb)
