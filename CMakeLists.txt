cmake_minimum_required(VERSION 3.2)
list(APPEND CMAKE_PREFIX_PATH "${CMAKE_CURRENT_LIST_DIR}/deps")
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")

project(callfwd)

set(CMAKE_CXX_STANDARD 14)
if(CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_GNUCXX)
  add_compile_options("-Wall" "-Wextra" "-pedantic" "-Wno-unused-parameter")
endif()

find_package(Gflags REQUIRED MODULE)
find_package(proxygen REQUIRED)
include(ProxygenTest)

include_directories(${CMAKE_SOURCE_DIR})

add_subdirectory(lib)
add_subdirectory(echo)
add_subdirectory(callfwd)
