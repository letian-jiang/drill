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
set(Parquet_FOUND TRUE)
if(NOT TARGET Parquet::parquet_shared)
  add_library(Parquet::parquet_shared SHARED IMPORTED)
  set_target_properties(Parquet::parquet_shared PROPERTIES
    IMPORTED_LOCATION "${DRILL_ICEBERG_ARROW_DIR}/libparquet.so.2500"
    INTERFACE_INCLUDE_DIRECTORIES "${DRILL_ICEBERG_ARROW_DIR}/include"
    INTERFACE_LINK_LIBRARIES Arrow::arrow_shared)
endif()
