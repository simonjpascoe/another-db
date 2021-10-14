# CMakeList.txt : CMake project for testing, include source and define
# project specific logic here.
#
cmake_minimum_required (VERSION 3.8)

# Add source to this project's executable.
add_executable (testing "testing.cpp" "testing.h")
set_property (TARGET testing PROPERTY CXX_STANDARD 20)

target_link_libraries (testing LINK_PUBLIC tabular)


# TODO: Add tests and install targets if needed.
