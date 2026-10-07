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
project(drill_native_execution LANGUAGES C CXX)
set(CMAKE_POLICY_VERSION_MINIMUM 3.5)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
find_package(JNI REQUIRED)
get_filename_component(DRILL_NATIVE_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}" REALPATH)
get_filename_component(DRILL_NATIVE_REPOSITORY_DIR "${DRILL_NATIVE_SOURCE_DIR}/.." ABSOLUTE)
set(DRILL_NATIVE_CACHE "${DRILL_NATIVE_REPOSITORY_DIR}/.tools/native-runtime" CACHE PATH "Prepared dependencies")
get_filename_component(DRILL_NATIVE_TOOLS_DIR "${DRILL_NATIVE_CACHE}/.." ABSOLUTE)
set(DRILL_NATIVE_DEPENDENCY_PREFIX "" CACHE PATH "Installed Boost and other Folly development dependencies")
if(DRILL_NATIVE_DEPENDENCY_PREFIX)
  list(PREPEND CMAKE_PREFIX_PATH "${DRILL_NATIVE_DEPENDENCY_PREFIX}")
endif()
set(DRILL_NATIVE_SOURCES "${DRILL_NATIVE_CACHE}/sources")
find_path(DRILL_RE2_INCLUDE_DIR re2/re2.h
  HINTS "${DRILL_NATIVE_DEPENDENCY_PREFIX}/include"
        "${DRILL_NATIVE_TOOLS_DIR}/iceberg-toolchain/env/include"
  REQUIRED)

# Generate headers using the binary's matching Folly source. Only the runtime
# target is compiled; the bootstrap libraries already contain these dependencies.
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
set(WITH_GFLAGS OFF CACHE BOOL "" FORCE)
set(WITH_GTEST OFF CACHE BOOL "" FORCE)
add_subdirectory("${DRILL_NATIVE_SOURCES}/glog" glog EXCLUDE_FROM_ALL)
set(GFLAGS_BUILD_TESTING OFF CACHE BOOL "" FORCE)
set(GFLAGS_BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
add_subdirectory("${DRILL_NATIVE_SOURCES}/gflags" gflags EXCLUDE_FROM_ALL)
set(FOLLY_HAVE_INT128_T ON CACHE BOOL "" FORCE)
set(FOLLY_HAVE_LIBURING OFF CACHE BOOL "" FORCE)
set(FOLLY_USE_JEMALLOC OFF CACHE BOOL "" FORCE)
set(FOLLY_BUILD_TESTS OFF CACHE BOOL "" FORCE)
add_subdirectory("${DRILL_NATIVE_SOURCES}/folly" folly EXCLUDE_FROM_ALL)

# Folly configures directory-wide dependency includes. Restore the target's
# ordered matching headers so unrelated installed glog headers cannot win.
set_property(DIRECTORY PROPERTY INCLUDE_DIRECTORIES "")
add_library(drill_native_dependencies INTERFACE)
target_include_directories(drill_native_dependencies SYSTEM BEFORE INTERFACE
  ${JNI_INCLUDE_DIRS}
  "${DRILL_NATIVE_SOURCES}/velox"
  "${DRILL_NATIVE_SOURCES}/folly" "${CMAKE_CURRENT_BINARY_DIR}/folly"
  "${CMAKE_CURRENT_BINARY_DIR}/glog" "${CMAKE_CURRENT_BINARY_DIR}/glog/include"
  "${DRILL_NATIVE_SOURCES}/glog/src"
  "${CMAKE_CURRENT_BINARY_DIR}/gflags/include"
  "${DRILL_NATIVE_SOURCES}/fmt/include"
  "${DRILL_NATIVE_SOURCES}/xsimd/include"
  ${DOUBLE_CONVERSION_INCLUDE_DIR} ${Boost_INCLUDE_DIRS}
  "${DRILL_NATIVE_DEPENDENCY_PREFIX}/include")
# WindowFunction's public declarations transitively include RE2. Keep this
# include fallback on the binder source, after all matching Velox/Folly and
# system headers, so an SDK prefix cannot change unrelated dependency headers.
set_source_files_properties(src/plan/ExpressionBinder.cpp PROPERTIES
  COMPILE_OPTIONS "-idirafter;${DRILL_RE2_INCLUDE_DIR}")
target_compile_definitions(drill_native_dependencies INTERFACE
  FMT_HEADER_ONLY GFLAGS_IS_A_DLL=0 _GLIBCXX_USE_CXX11_ABI=0)
target_compile_options(drill_native_dependencies INTERFACE
  -mavx2 -mfma -mavx -mf16c -mlzcnt -mbmi2 -Wall -Wextra -Werror=return-type)
enable_testing()

# The pure execution path has no Java entrypoints, JVM linkage or Velox4J Query.
add_library(drill_native_execution STATIC
  src/velox/operators/DrillOperators.cpp
  src/plan/FragmentPlanConverter.cpp
  src/plan/DrillSchema.cpp
  src/plan/ExpressionBinder.cpp
  src/execution/VeloxRuntime.cpp
  src/execution/NativeEngine.cpp
  src/columnar/ColumnarBatch.cpp
  src/columnar/DrillBatchMetadata.cpp
  src/scan/NativeScanRegistry.cpp
  src/scan/iceberg/IcebergScan.cpp
  src/scan/iceberg/IcebergArrowBatch.cpp
  src/scan/iceberg/IcebergOutput.cpp
  src/scan/jni/JniPluginScan.cpp
  src/exchange/DrillHash.cpp
  src/exchange/ReceiverInbox.cpp
  src/exchange/SenderBuffer.cpp
  src/worker/MinorTaskRegistry.cpp
  src/velox/functions/DrillDecimal.cpp
  src/velox/functions/DrillNumeric.cpp
  src/velox/functions/DrillDict.cpp)
target_include_directories(drill_native_execution SYSTEM PRIVATE
  $<TARGET_PROPERTY:drill_native_dependencies,INTERFACE_INCLUDE_DIRECTORIES>)
target_include_directories(drill_native_execution PUBLIC src)
target_compile_definitions(drill_native_execution PUBLIC FMT_HEADER_ONLY GFLAGS_IS_A_DLL=0 _GLIBCXX_USE_CXX11_ABI=0)
target_compile_options(drill_native_execution PRIVATE
  $<TARGET_PROPERTY:drill_native_dependencies,INTERFACE_COMPILE_OPTIONS>)
target_link_libraries(drill_native_execution PUBLIC
  drill_native_dependencies
  "${DRILL_NATIVE_CACHE}/lib/libvelox.so"
  "${DRILL_NATIVE_CACHE}/lib/libfolly.so.0.58.0-dev"
  "${DRILL_NATIVE_CACHE}/lib/libglog.so.1"
  "${DRILL_NATIVE_CACHE}/lib/libdouble-conversion.so.1" pthread dl)
add_executable(native_plan_execution_test tests/execution/NativePlanExecutionTest.cpp)
target_include_directories(native_plan_execution_test SYSTEM PRIVATE
  $<TARGET_PROPERTY:drill_native_dependencies,INTERFACE_INCLUDE_DIRECTORIES>)
target_compile_options(native_plan_execution_test PRIVATE
  $<TARGET_PROPERTY:drill_native_dependencies,INTERFACE_COMPILE_OPTIONS>)
target_link_libraries(native_plan_execution_test PRIVATE drill_native_execution)
set_target_properties(native_plan_execution_test PROPERTIES BUILD_RPATH "${DRILL_NATIVE_CACHE}/lib")
add_test(NAME native_plan_execution COMMAND native_plan_execution_test)

add_library(drill_native_protocol STATIC src/protocol/Protobuf.cpp src/protocol/RpcMessage.cpp src/protocol/RpcTransport.cpp)
target_include_directories(drill_native_protocol PUBLIC src)
target_compile_definitions(drill_native_protocol PUBLIC _GLIBCXX_USE_CXX11_ABI=0)
add_executable(native_rpc_codec_test tests/protocol/RpcCodecTest.cpp)
target_link_libraries(native_rpc_codec_test PRIVATE drill_native_protocol)
add_test(NAME native_rpc_codec COMMAND native_rpc_codec_test)

target_link_libraries(drill_native_execution PUBLIC drill_native_protocol)

add_library(drill_native_engine SHARED src/jni/NativeEngineJni.cpp)
target_link_libraries(drill_native_engine PRIVATE drill_native_execution)
set_target_properties(drill_native_engine PROPERTIES
  BUILD_RPATH "${DRILL_NATIVE_CACHE}/lib")

target_link_libraries(drill_native_protocol PUBLIC pthread)

add_executable(native_worker_rpc_execution_test tests/worker/NativeWorkerRpcExecutionTest.cpp)
target_include_directories(native_worker_rpc_execution_test SYSTEM PRIVATE
  $<TARGET_PROPERTY:drill_native_dependencies,INTERFACE_INCLUDE_DIRECTORIES>)
target_compile_options(native_worker_rpc_execution_test PRIVATE
  $<TARGET_PROPERTY:drill_native_dependencies,INTERFACE_COMPILE_OPTIONS>)
target_link_libraries(native_worker_rpc_execution_test PRIVATE drill_native_execution)
set_target_properties(native_worker_rpc_execution_test PROPERTIES BUILD_RPATH "${DRILL_NATIVE_CACHE}/lib")
add_test(NAME native_worker_rpc_execution COMMAND native_worker_rpc_execution_test)
set_tests_properties(native_worker_rpc_execution PROPERTIES TIMEOUT 60)

add_executable(native_engine_test tests/execution/NativeEngineTest.cpp)
target_link_libraries(native_engine_test PRIVATE drill_native_execution)
set_target_properties(native_engine_test PROPERTIES BUILD_RPATH "${DRILL_NATIVE_CACHE}/lib")
add_test(NAME native_engine COMMAND native_engine_test)
set_tests_properties(native_engine PROPERTIES TIMEOUT 30)

add_executable(native_plan_lifetime_test tests/execution/PlanLifetimeTest.cpp)
target_link_libraries(native_plan_lifetime_test PRIVATE drill_native_execution)
set_target_properties(native_plan_lifetime_test PROPERTIES BUILD_RPATH "${DRILL_NATIVE_CACHE}/lib")
add_test(NAME native_plan_lifetime COMMAND native_plan_lifetime_test)
set_tests_properties(native_plan_lifetime PROPERTIES TIMEOUT 30)

add_executable(native_columnar_test tests/NativeColumnarTest.cpp)
target_link_libraries(native_columnar_test PRIVATE drill_native_execution)
set_target_properties(native_columnar_test PROPERTIES BUILD_RPATH "${DRILL_NATIVE_CACHE}/lib")
add_test(NAME native_columnar COMMAND native_columnar_test)

add_executable(native_map_test tests/NativeMapTest.cpp)
target_link_libraries(native_map_test PRIVATE drill_native_execution)
set_target_properties(native_map_test PROPERTIES BUILD_RPATH "${DRILL_NATIVE_CACHE}/lib")
add_test(NAME native_map COMMAND native_map_test)

add_executable(native_array_test tests/NativeArrayTest.cpp)
target_link_libraries(native_array_test PRIVATE drill_native_execution)
set_target_properties(native_array_test PROPERTIES BUILD_RPATH "${DRILL_NATIVE_CACHE}/lib")
add_test(NAME native_array COMMAND native_array_test)
add_executable(native_dict_test tests/NativeDictTest.cpp)
target_link_libraries(native_dict_test PRIVATE drill_native_execution)
set_target_properties(native_dict_test PROPERTIES BUILD_RPATH "${DRILL_NATIVE_CACHE}/lib")
add_test(NAME native_dict COMMAND native_dict_test)

add_executable(native_window_test tests/plan/WindowExecutionTest.cpp)
target_link_libraries(native_window_test PRIVATE drill_native_execution)
set_target_properties(native_window_test PROPERTIES BUILD_RPATH "${DRILL_NATIVE_CACHE}/lib")
add_test(NAME native_window COMMAND native_window_test)
set_tests_properties(native_window PROPERTIES TIMEOUT 30)

add_executable(native_topn_test tests/plan/TopNExecutionTest.cpp)
target_link_libraries(native_topn_test PRIVATE drill_native_execution)
set_target_properties(native_topn_test PROPERTIES BUILD_RPATH "${DRILL_NATIVE_CACHE}/lib")
add_test(NAME native_topn COMMAND native_topn_test)
set_tests_properties(native_topn PROPERTIES TIMEOUT 30)

add_executable(native_local_partition_test tests/plan/LocalPartitionExecutionTest.cpp)
target_link_libraries(native_local_partition_test PRIVATE drill_native_execution)
set_target_properties(native_local_partition_test PROPERTIES BUILD_RPATH "${DRILL_NATIVE_CACHE}/lib")
add_test(NAME native_local_partition COMMAND native_local_partition_test)
set_tests_properties(native_local_partition PROPERTIES TIMEOUT 30)

add_executable(native_join_batch_test tests/plan/JoinBatchExecutionTest.cpp)
target_link_libraries(native_join_batch_test PRIVATE drill_native_execution)
set_target_properties(native_join_batch_test PROPERTIES BUILD_RPATH "${DRILL_NATIVE_CACHE}/lib")
add_test(NAME native_join_batch COMMAND native_join_batch_test)
set_tests_properties(native_join_batch PROPERTIES TIMEOUT 30)

add_executable(native_iceberg_arrow_batch_test tests/scan/IcebergArrowBatchTest.cpp)
target_link_libraries(native_iceberg_arrow_batch_test PRIVATE drill_native_execution)
set_target_properties(native_iceberg_arrow_batch_test PROPERTIES BUILD_RPATH "${DRILL_NATIVE_CACHE}/lib")
add_test(NAME native_iceberg_arrow_batch COMMAND native_iceberg_arrow_batch_test)
add_executable(native_iceberg_nested_arrow_batch_test tests/scan/IcebergNestedArrowBatchTest.cpp)
target_link_libraries(native_iceberg_nested_arrow_batch_test PRIVATE drill_native_execution)
set_target_properties(native_iceberg_nested_arrow_batch_test PROPERTIES BUILD_RPATH "${DRILL_NATIVE_CACHE}/lib")
add_test(NAME native_iceberg_nested_arrow_batch COMMAND native_iceberg_nested_arrow_batch_test)

add_executable(native_expression_semantics_test tests/plan/ExpressionSemanticsTest.cpp)
target_link_libraries(native_expression_semantics_test PRIVATE drill_native_execution)
set_target_properties(native_expression_semantics_test PROPERTIES BUILD_RPATH "${DRILL_NATIVE_CACHE}/lib")
add_test(NAME native_expression_semantics COMMAND native_expression_semantics_test)

add_executable(native_rpc_transport_test tests/protocol/RpcTransportTest.cpp)
target_link_libraries(native_rpc_transport_test PRIVATE drill_native_protocol)
add_test(NAME native_rpc_transport COMMAND native_rpc_transport_test)
set_tests_properties(native_rpc_transport PROPERTIES TIMEOUT 30)

add_executable(native_sender_test tests/execution/NativeSenderTest.cpp)
target_link_libraries(native_sender_test PRIVATE drill_native_execution)
set_target_properties(native_sender_test PROPERTIES BUILD_RPATH "${DRILL_NATIVE_CACHE}/lib")
add_test(NAME native_sender COMMAND native_sender_test)
set_tests_properties(native_sender PROPERTIES TIMEOUT 30)

# Isolated Java host exercises the real JNI adapter without plugin dependencies.
find_program(DRILL_NATIVE_JAVAC javac HINTS "${JAVA_INCLUDE_PATH}/../bin" REQUIRED)
find_library(DRILL_JNI_TEST_JVM jvm HINTS "${JAVA_INCLUDE_PATH}/../lib/server" NO_DEFAULT_PATH REQUIRED)
set(DRILL_JNI_TEST_CLASSES "${CMAKE_CURRENT_BINARY_DIR}/jni-test-classes")
set(DRILL_JNI_TEST_HOST "${DRILL_JNI_TEST_CLASSES}/org/apache/drill/exec/nativeexecution/scan/ScanHost.class")
add_custom_command(OUTPUT "${DRILL_JNI_TEST_HOST}"
  COMMAND ${CMAKE_COMMAND} -E make_directory "${DRILL_JNI_TEST_CLASSES}"
  COMMAND "${DRILL_NATIVE_JAVAC}" -d "${DRILL_JNI_TEST_CLASSES}"
    "${CMAKE_CURRENT_SOURCE_DIR}/tests/scan/java/org/apache/drill/exec/nativeexecution/scan/ScanHost.java"
  DEPENDS tests/scan/java/org/apache/drill/exec/nativeexecution/scan/ScanHost.java)
add_custom_target(native_jni_test_host DEPENDS "${DRILL_JNI_TEST_HOST}")
add_executable(native_jni_scan_test tests/scan/JniPluginScanTest.cpp)
add_dependencies(native_jni_scan_test native_jni_test_host)
target_link_libraries(native_jni_scan_test PRIVATE drill_native_execution)
set_target_properties(native_jni_scan_test PROPERTIES BUILD_RPATH "${DRILL_NATIVE_CACHE}/lib")
add_test(NAME native_jni_scan COMMAND native_jni_scan_test)
set_tests_properties(native_jni_scan PROPERTIES TIMEOUT 60
  ENVIRONMENT "DRILL_NATIVE_SCAN_CLASSPATH=${DRILL_JNI_TEST_CLASSES};DRILL_NATIVE_JVM_LIBRARY=${DRILL_JNI_TEST_JVM}")

find_program(DRILL_NATIVE_JAVA java HINTS "${JAVA_INCLUDE_PATH}/../bin" REQUIRED)
set(DRILL_ENGINE_JNI_TEST "${DRILL_JNI_TEST_CLASSES}/NativeEngineJniTest.class")
add_custom_command(OUTPUT "${DRILL_ENGINE_JNI_TEST}"
  COMMAND "${DRILL_NATIVE_JAVAC}" -cp "${DRILL_JNI_TEST_CLASSES}" -d "${DRILL_JNI_TEST_CLASSES}"
    "${CMAKE_CURRENT_SOURCE_DIR}/../exec/java-exec/src/main/java/org/apache/drill/exec/nativeexecution/NativeEngine.java"
    "${CMAKE_CURRENT_SOURCE_DIR}/tests/execution/java/NativeEngineJniTest.java"
  DEPENDS "${DRILL_JNI_TEST_HOST}"
    ../exec/java-exec/src/main/java/org/apache/drill/exec/nativeexecution/NativeEngine.java
    tests/execution/java/NativeEngineJniTest.java)
add_custom_target(native_engine_jni_test ALL DEPENDS "${DRILL_ENGINE_JNI_TEST}")
add_test(NAME native_engine_jni COMMAND "${DRILL_NATIVE_JAVA}"
  --add-opens=java.base/java.nio=ALL-UNNAMED -cp "${DRILL_JNI_TEST_CLASSES}"
  NativeEngineJniTest "$<TARGET_FILE:drill_native_engine>")
set_tests_properties(native_engine_jni PROPERTIES TIMEOUT 30)

# Production Java plugin/classpath integration, invoked by run_generic_plugin_rpc.py.
# Kept separate from the isolated mock-JVM CTests: requires built Drill jars and PyArrow data fixtures.
add_executable(native_generic_plugin_rpc_test tests/worker/GenericPluginRpcTest.cpp)
target_link_libraries(native_generic_plugin_rpc_test PRIVATE drill_native_execution)
set_target_properties(native_generic_plugin_rpc_test PROPERTIES BUILD_RPATH "${DRILL_NATIVE_CACHE}/lib")

add_executable(native_engine_rpc_test tests/execution/NativeEngineRpcTest.cpp)
target_link_libraries(native_engine_rpc_test PRIVATE drill_native_execution)
set_target_properties(native_engine_rpc_test PROPERTIES BUILD_RPATH "${DRILL_NATIVE_CACHE}/lib")
add_test(NAME native_engine_rpc COMMAND native_engine_rpc_test)
set_tests_properties(native_engine_rpc PROPERTIES TIMEOUT 40)

# Public native scan module example. Share the engine API and dependency ABI.
add_library(native_range_scan_plugin MODULE examples/scan/RangeScanPlugin.cpp)
target_include_directories(native_range_scan_plugin PRIVATE src)
target_link_libraries(native_range_scan_plugin PRIVATE drill_native_engine drill_native_dependencies)
target_compile_options(native_range_scan_plugin PRIVATE
  $<TARGET_PROPERTY:drill_native_dependencies,INTERFACE_COMPILE_OPTIONS>)
set_target_properties(native_range_scan_plugin PROPERTIES
  BUILD_RPATH "${DRILL_NATIVE_CACHE}/lib;${CMAKE_CURRENT_BINARY_DIR}")
add_executable(native_scan_plugin_test tests/execution/NativeScanPluginTest.cpp)
target_link_libraries(native_scan_plugin_test PRIVATE drill_native_engine drill_native_execution)
set_target_properties(native_scan_plugin_test PROPERTIES
  BUILD_RPATH "${DRILL_NATIVE_CACHE}/lib;${CMAKE_CURRENT_BINARY_DIR}")
add_test(NAME native_scan_plugin COMMAND native_scan_plugin_test)
set_tests_properties(native_scan_plugin PROPERTIES TIMEOUT 30
  ENVIRONMENT "DRILL_NATIVE_SCAN_PLUGINS=$<TARGET_FILE:native_range_scan_plugin>")
add_executable(native_drill_schema_test tests/plan/DrillSchemaTest.cpp)
target_link_libraries(native_drill_schema_test PRIVATE drill_native_execution)
set_target_properties(native_drill_schema_test PROPERTIES BUILD_RPATH "${DRILL_NATIVE_CACHE}/lib")
add_test(NAME native_drill_schema COMMAND native_drill_schema_test)
set_tests_properties(native_drill_schema PROPERTIES TIMEOUT 30)
