# CMakeList.txt : CMake project for testing, include source and define
# project specific logic here.
#
cmake_minimum_required (VERSION 3.8)

add_library (tabular "tabular.h" "tabular.cpp" "schema.h" "schema.cpp"    "central.h")
set_property (TARGET tabular PROPERTY CXX_STANDARD 20)

target_include_directories (tabular PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})