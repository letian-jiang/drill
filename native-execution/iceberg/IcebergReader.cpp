// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0 .
#include "IcebergReaderAbi.h"
#include "IcebergTaskReader.h"
#include "iceberg/arrow/arrow_register.h"
#include "iceberg/avro/avro_register.h"
#include "iceberg/file_reader.h"
#include "iceberg/parquet/parquet_register.h"
#include <algorithm>
#include <arrow/compute/initialize.h>
#include <arrow/memory_pool.h>
#include <cstring>
#include <memory>
#include <mutex>
#include <nlohmann/json.hpp>
#include <stdexcept>

namespace {
template <typename T> T take(iceberg::Result<T> value) {
  if (!value)
    throw std::runtime_error(value.error().message);
  return std::move(*value);
}
void fail(const char *message, char *error, size_t size) noexcept {
  if (size) {
    auto length = std::min(std::strlen(message), size - 1);
    std::memcpy(error, message, length);
    error[length] = 0;
  }
}
struct Reader {
  std::unique_ptr<iceberg::Reader> parquet;
};
} // namespace

extern "C" int drill_iceberg_open(const char *json, void **reader, char *error,
                                  size_t size) {
  *reader = nullptr;
  try {
    static std::once_flag registration;
    std::call_once(registration, [] {
      auto status = arrow::compute::Initialize();
      if (!status.ok())
        throw std::runtime_error(status.ToString());
      iceberg::arrow::RegisterAll();
      iceberg::avro::RegisterAll();
      iceberg::parquet::RegisterAll();
    });
    auto config = nlohmann::json::parse(json);
    auto result = std::make_unique<Reader>();
    result->parquet = drill::icebergsdk::openTask(config);
    *reader = result.release();
    return 0;
  } catch (const std::exception &e) {
    fail(e.what(), error, size);
  } catch (...) {
    fail("Unknown Iceberg open error", error, size);
  }
  return -1;
}

extern "C" int drill_iceberg_next(void *reader, ArrowSchema *schema,
                                  ArrowArray *array, char *error, size_t size) {
  *schema = {};
  *array = {};
  try {
    auto &parquet = static_cast<Reader *>(reader)->parquet;
    auto batch = take(parquet->Next());
    if (!batch)
      return 0;
    *array = std::move(*batch);
    *schema = take(parquet->Schema());
    return 1;
  } catch (const std::exception &e) {
    fail(e.what(), error, size);
  } catch (...) {
    fail("Unknown Iceberg read error", error, size);
  }
  if (array->release)
    array->release(array);
  if (schema->release)
    schema->release(schema);
  return -1;
}

extern "C" void drill_iceberg_close(void *reader) {
  delete static_cast<Reader *>(reader);
}

extern "C" long long drill_iceberg_memory_bytes() {
  return arrow::default_memory_pool()->bytes_allocated();
}
