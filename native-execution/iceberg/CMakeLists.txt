# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
cmake_minimum_required(VERSION 3.28)
project(DrillIcebergReader LANGUAGES C CXX)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN ON)
set(ICEBERG_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(ICEBERG_BUILD_REST OFF CACHE BOOL "" FORCE)
set(ICEBERG_BUILD_STATIC ON CACHE BOOL "" FORCE)
set(ICEBERG_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(ICEBERG_SPDLOG OFF CACHE BOOL "" FORCE)
set(DRILL_ICEBERG_ARROW_DIR "" CACHE PATH "PyArrow 25 standalone C++ libraries and headers")
set(Arrow_DIR "${CMAKE_CURRENT_SOURCE_DIR}/arrow-packages/Arrow" CACHE PATH "" FORCE)
set(Parquet_DIR "${CMAKE_CURRENT_SOURCE_DIR}/arrow-packages/Parquet" CACHE PATH "" FORCE)
get_filename_component(DRILL_ICEBERG_MODULE_DIR "${CMAKE_CURRENT_SOURCE_DIR}" REALPATH)
get_filename_component(DRILL_ICEBERG_REPOSITORY_DIR "${DRILL_ICEBERG_MODULE_DIR}/../.." ABSOLUTE)
set(DRILL_ICEBERG_SOURCE "${DRILL_ICEBERG_REPOSITORY_DIR}/.tools/iceberg-cpp" CACHE PATH "Pinned iceberg-cpp checkout")
add_subdirectory("${DRILL_ICEBERG_SOURCE}" sdk)
add_library(drill_iceberg_arrow_compute SHARED IMPORTED)
set_target_properties(drill_iceberg_arrow_compute PROPERTIES
  IMPORTED_LOCATION "${DRILL_ICEBERG_ARROW_DIR}/libarrow_compute.so.2500"
  INTERFACE_INCLUDE_DIRECTORIES "${DRILL_ICEBERG_ARROW_DIR}/include")
add_library(drill_iceberg_reader SHARED IcebergReader.cpp IcebergTaskReader.cpp)
target_include_directories(drill_iceberg_reader PRIVATE "${DRILL_ICEBERG_SOURCE}/src" "${CMAKE_CURRENT_BINARY_DIR}/sdk/src" ../src)
target_link_libraries(drill_iceberg_reader PRIVATE iceberg_bundle_static drill_iceberg_arrow_compute)
target_link_options(drill_iceberg_reader PRIVATE "-Wl,--exclude-libs,ALL")
