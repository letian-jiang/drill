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
# Use the standalone Arrow libraries bundled with the pinned PyArrow wheel.
# They avoid unrelated cloud/ORC/gflags dependencies in the native worker.
set(Arrow_FOUND TRUE)
if(NOT TARGET Arrow::arrow_shared)
  add_library(Arrow::arrow_shared SHARED IMPORTED)
  set_target_properties(Arrow::arrow_shared PROPERTIES
    IMPORTED_LOCATION "${DRILL_ICEBERG_ARROW_DIR}/libarrow.so.2500"
    INTERFACE_INCLUDE_DIRECTORIES "${DRILL_ICEBERG_ARROW_DIR}/include")
endif()
