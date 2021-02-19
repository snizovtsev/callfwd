cmake_minimum_required(VERSION 3.2)
list(APPEND CMAKE_PREFIX_PATH "${CMAKE_CURRENT_LIST_DIR}/deps")
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")

project(callfwd)

find_package(Gflags REQUIRED MODULE)
find_package(glog REQUIRED MODULE)
find_package(proxygen REQUIRED)
find_package(TBB REQUIRED)
pkg_check_modules(SYSTEMD REQUIRED libsystemd)
include(ProxygenTest)

#set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD 17)
add_definitions(-DHAVE_STD_PARALLEL)
if(CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_GNUCXX)
  add_compile_options("-Wall" "-Wextra" "-pedantic" "-Wno-unused-parameter")
endif()

include_directories(${CMAKE_SOURCE_DIR})
add_subdirectory(lib)
add_subdirectory(callfwd)
add_subdirectory(stir)
