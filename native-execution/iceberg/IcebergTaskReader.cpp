/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#include "IcebergTaskReader.h"
#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/arrow/literal_util_internal.h"
#include "iceberg/arrow_c_data_guard_internal.h"
#include "iceberg/arrow_c_data_util_internal.h"
#include "iceberg/data/delete_filter.h"
#include "iceberg/expression/literal.h"
#include "iceberg/json_serde_internal.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/compute/api.h>
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <unordered_map>

namespace drill::icebergsdk {
namespace {
template <class T> T take(iceberg::Result<T> result) {
  if (!result)
    throw std::runtime_error(result.error().message);
  return std::move(*result);
}
template <class T> T take(::arrow::Result<T> result) {
  if (!result.ok())
    throw std::runtime_error(result.status().ToString());
  return std::move(*result);
}
void check(::arrow::Status result) {
  if (!result.ok())
    throw std::runtime_error(result.ToString());
}
using Constants = std::unordered_map<int32_t, iceberg::Literal>;
using Fields = std::span<const iceberg::SchemaField>;

std::vector<iceberg::SchemaField> defaults(Fields fields,
                                           const Constants &constants) {
  std::vector<iceberg::SchemaField> result;
  for (auto field : fields) {
    if (auto constant = constants.find(field.field_id());
        constant != constants.end()) {
      field = field.WithInitialDefault(
          std::make_shared<iceberg::Literal>(constant->second));
    } else if (auto nested = std::dynamic_pointer_cast<iceberg::StructType>(
                   field.type())) {
      field = field.WithType(std::make_shared<iceberg::StructType>(
          defaults(nested->fields(), constants)));
    }
    result.push_back(std::move(field));
  }
  return result;
}
std::shared_ptr<::arrow::Schema> arrowSchema(const iceberg::Schema &schema) {
  ArrowSchema raw{};
  auto status = iceberg::ToArrowSchema(schema, &raw);
  if (!status)
    throw std::runtime_error(status.error().message);
  iceberg::internal::ArrowSchemaGuard guard(&raw);
  return take(::arrow::ImportSchema(&raw));
}
size_t fieldIndex(const Fields &fields, int32_t id) {
  for (size_t i = 0; i < fields.size(); ++i)
    if (fields[i].field_id() == id)
      return i;
  throw std::runtime_error("Missing planned Iceberg field ID " +
                           std::to_string(id));
}
std::shared_ptr<::arrow::Array>
column(const iceberg::SchemaField &wanted, const iceberg::SchemaField &provided,
       const std::shared_ptr<::arrow::Field> &type,
       std::shared_ptr<::arrow::Array> input, const Constants &constants) {
  if (auto constant = constants.find(wanted.field_id());
      constant != constants.end())
    return take(iceberg::arrow::MakeDefaultArray(
        constant->second, type->type(), input->length(),
        ::arrow::default_memory_pool()));
  auto wantedStruct =
      std::dynamic_pointer_cast<iceberg::StructType>(wanted.type());
  if (!wantedStruct)
    return input;
  auto providedStruct =
      std::dynamic_pointer_cast<iceberg::StructType>(provided.type());
  if (!providedStruct || input->type_id() != ::arrow::Type::STRUCT)
    throw std::runtime_error("Iceberg struct projection mismatch");
  auto source = std::static_pointer_cast<::arrow::StructArray>(input);
  auto requested = std::static_pointer_cast<::arrow::StructType>(type->type());
  ::arrow::ArrayVector children;
  ::arrow::FieldVector types;
  for (size_t i = 0; i < wantedStruct->fields().size(); ++i) {
    auto index = fieldIndex(providedStruct->fields(),
                            wantedStruct->fields()[i].field_id());
    children.push_back(
        column(wantedStruct->fields()[i], providedStruct->fields()[index],
               requested->field(i), source->field(index), constants));
    types.push_back(requested->field(i)->WithType(children.back()->type()));
  }
  // Rebuild only the struct wrapper. Keep the original parent validity and
  // child owners; extra equality-delete fields must not escape the projection.
  return std::make_shared<::arrow::StructArray>(
      ::arrow::struct_(types), source->length(), children,
      source->null_bitmap(), source->null_count(), source->offset());
}
std::shared_ptr<::arrow::RecordBatch>
project(const iceberg::Schema &wanted, const iceberg::Schema &provided,
        const std::shared_ptr<::arrow::Schema> &type,
        const std::shared_ptr<::arrow::RecordBatch> &input,
        const Constants &constants) {
  ::arrow::ArrayVector arrays;
  ::arrow::FieldVector fields;
  for (size_t i = 0; i < wanted.fields().size(); ++i) {
    auto index = fieldIndex(provided.fields(), wanted.fields()[i].field_id());
    arrays.push_back(column(wanted.fields()[i], provided.fields()[index],
                            type->field(i), input->column(index), constants));
    fields.push_back(type->field(i)->WithType(arrays.back()->type()));
  }
  return ::arrow::RecordBatch::Make(::arrow::schema(fields), input->num_rows(),
                                    arrays);
}
class TaskReader final : public iceberg::Reader {
public:
  TaskReader(std::unique_ptr<iceberg::Reader> source,
             std::unique_ptr<iceberg::DeleteFilter> filter,
             std::shared_ptr<iceberg::Schema> required,
             std::shared_ptr<iceberg::Schema> expected, Constants constants)
      : source_(std::move(source)), filter_(std::move(filter)),
        required_(std::move(required)), expected_(std::move(expected)),
        constants_(std::move(constants)),
        requiredArrow_(arrowSchema(*required_)),
        outputArrow_(arrowSchema(*expected_)) {}
  iceberg::Status Open(const iceberg::ReaderOptions &) override {
    return iceberg::InvalidArgument("Planned TaskReader is already open");
  }
  iceberg::Status Close() override { return source_->Close(); }
  iceberg::Result<std::unordered_map<std::string, std::string>>
  Metadata() override {
    return source_->Metadata();
  }
  iceberg::Result<ArrowSchema> Schema() override {
    ArrowSchema schema{};
    auto status = ::arrow::ExportSchema(*outputArrow_, &schema);
    if (!status.ok())
      return iceberg::InvalidArgument("{}", status.ToString());
    return schema;
  }
  iceberg::Result<std::optional<ArrowArray>> Next() override {
    try {
      while (true) {
        auto next = take(source_->Next());
        if (!next)
          return std::nullopt;
        ArrowArray input = std::move(*next);
        iceberg::internal::ArrowArrayGuard inputGuard(&input);
        ArrowSchema schema = take(source_->Schema());
        iceberg::internal::ArrowSchemaGuard schemaGuard(&schema);
        auto batch = take(::arrow::ImportRecordBatch(&input, &schema));
        batch =
            project(*required_, *required_, requiredArrow_, batch, constants_);
        std::optional<iceberg::AliveRowSelection> alive;
        if (filter_) {
          ArrowArray raw{};
          ArrowSchema rawSchema{};
          iceberg::internal::ArrowArrayGuard arrayGuard(&raw);
          iceberg::internal::ArrowSchemaGuard guard(&rawSchema);
          check(::arrow::ExportRecordBatch(*batch, &raw, &rawSchema));
          alive = take(filter_->ComputeAliveRows(rawSchema, raw));
          if (alive->empty())
            continue;
        }
        batch = project(*expected_, *required_, outputArrow_, batch, {});
        if (alive && alive->alive_count() != batch->num_rows()) {
          ::arrow::Int32Builder builder;
          check(builder.AppendValues(alive->indices));
          auto indices = take(builder.Finish());
          batch = take(::arrow::compute::Take(::arrow::Datum(batch),
                                              ::arrow::Datum(indices)))
                      .record_batch();
        }
        outputArrow_ = batch->schema();
        ArrowArray output{};
        check(::arrow::ExportRecordBatch(*batch, &output));
        return output;
      }
    } catch (const std::exception &e) {
      return iceberg::InvalidArgument("Planned task read: {}", e.what());
    }
  }

private:
  std::unique_ptr<iceberg::Reader> source_;
  std::unique_ptr<iceberg::DeleteFilter> filter_;
  std::shared_ptr<iceberg::Schema> required_, expected_;
  Constants constants_;
  std::shared_ptr<::arrow::Schema> requiredArrow_, outputArrow_;
};
} // namespace
std::unique_ptr<iceberg::Reader> openTask(const nlohmann::json &config) {
  std::shared_ptr<iceberg::Schema> expected =
      take(iceberg::SchemaFromJson(config.at("schema")));
  std::shared_ptr<iceberg::FileIO> io =
      iceberg::arrow::ArrowFileSystemFileIO::MakeLocalFileIO();
  Constants constants;
  for (const auto &entry : config.value("constants", nlohmann::json::array())) {
    std::shared_ptr<iceberg::Type> type =
        take(iceberg::TypeFromJson(entry.at("type")));
    auto primitive = std::dynamic_pointer_cast<iceberg::PrimitiveType>(type);
    if (!primitive)
      throw std::runtime_error("Identity partition must be primitive");
    auto literal =
        entry.at("value").is_null()
            ? iceberg::Literal::Null(primitive)
            : take(iceberg::Literal::Deserialize(
                  entry.at("value").get<std::vector<uint8_t>>(), primitive));
    if (!constants
             .emplace(entry.at("fieldId").get<int32_t>(), std::move(literal))
             .second)
      throw std::runtime_error("Duplicate identity partition source ID");
  }
  std::unique_ptr<iceberg::DeleteFilter> filter;
  auto required = expected;
  if (config.contains("deletes") && !config.at("deletes").empty()) {
    std::shared_ptr<iceberg::Schema> table =
        take(iceberg::SchemaFromJson(config.at("tableSchema")));
    std::vector<std::shared_ptr<iceberg::Schema>> history;
    for (const auto &schema :
         config.value("tableSchemas", nlohmann::json::array()))
      history.push_back(take(iceberg::SchemaFromJson(schema)));
    std::vector<std::shared_ptr<iceberg::DataFile>> deletes;
    for (const auto &entry : config.at("deletes")) {
      auto file = std::make_shared<iceberg::DataFile>();
      file->file_path = entry.at("path").get<std::string>();
      auto format = entry.at("format").get<std::string>();
      if (format != "PARQUET" && format != "AVRO")
        throw std::runtime_error("Unsupported delete format");
      file->file_format = format == "PARQUET"
                              ? iceberg::FileFormatType::kParquet
                              : iceberg::FileFormatType::kAvro;
      auto content = entry.at("content").get<int>();
      if (content != 1 && content != 2)
        throw std::runtime_error("Invalid delete content");
      file->content = static_cast<iceberg::DataFile::Content>(content);
      file->file_size_in_bytes = entry.at("fileSize").get<int64_t>();
      file->record_count = entry.at("recordCount").get<int64_t>();
      file->equality_ids = entry.at("equalityIds").get<std::vector<int32_t>>();
      deletes.push_back(std::move(file));
    }
    filter = take(iceberg::DeleteFilter::Make(
        config.at("dataFilePath").get<std::string>(), deletes, table, expected,
        io, history));
    required = filter->RequiredSchema();
  }
  iceberg::ReaderOptions options;
  options.path = config.at("path").get<std::string>();
  options.length = config.at("fileSize").get<size_t>();
  options.split = iceberg::Split{config.at("start").get<size_t>(),
                                 config.at("length").get<size_t>()};
  options.projection = constants.empty()
                           ? required
                           : std::make_shared<iceberg::Schema>(
                                 defaults(required->fields(), constants));
  options.io = io;
  options.properties = iceberg::ReaderProperties::FromMap(
      {{"read.batch-size", std::to_string(config.value("batchSize", 8192))}});
  auto source = take(iceberg::ReaderFactoryRegistry::Open(
      iceberg::FileFormatType::kParquet, options));
  if (!filter && constants.empty())
    return source;
  return std::make_unique<TaskReader>(std::move(source), std::move(filter),
                                      required, expected, std::move(constants));
}
} // namespace drill::icebergsdk
