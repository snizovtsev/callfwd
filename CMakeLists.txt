cmake_minimum_required(VERSION 3.2)
list(APPEND CMAKE_PREFIX_PATH "${CMAKE_CURRENT_LIST_DIR}/deps")
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")

project(callfwd)

set(CMAKE_CXX_STANDARD 14)

find_package(Gflags REQUIRED MODULE)
find_package(proxygen REQUIRED)
include(ProxygenTest)

include_directories(${CMAKE_SOURCE_DIR})

add_subdirectory(lib)
add_subdirectory(echo)
