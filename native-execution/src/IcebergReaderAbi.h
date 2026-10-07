// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0 .
#pragma once
#include <stddef.h>
struct ArrowArray;
struct ArrowSchema;
// Only C scalars and Arrow C Data cross the SDK/Velox ABI boundary. Every
// allocation is freed by its originating module, including Arrow callbacks.
extern "C" {
__attribute__((visibility("default"))) int
drill_iceberg_open(const char *json, void **reader, char *error, size_t size);
__attribute__((visibility("default"))) int
drill_iceberg_next(void *reader, ArrowSchema *schema, ArrowArray *array,
                   char *error, size_t size);
__attribute__((visibility("default"))) void drill_iceberg_close(void *reader);
__attribute__((visibility("default"))) long long drill_iceberg_memory_bytes();
}
