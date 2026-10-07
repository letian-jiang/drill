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
#include "columnar/ColumnarBatch.h"
#include "columnar/DrillBatchMetadata.h"
#include "execution/VeloxRuntime.h"
#include "plan/FragmentPlanConverter.h"
#include "protocol/Protobuf.h"
#include <folly/base64.h>
#include <folly/json.h>
#include <iostream>
#include <velox/vector/FlatVector.h>
using namespace drill::nativeexec;
int main() {
  try {
    VeloxRuntime runtime(1);
    auto pool = memory::memoryManager()->addRootPool("schema-test");
    auto leaf = pool->addLeafChild("plan");
    auto fields = folly::parseJson(R"PLAN([
      {"name":"a","minor":"INT","optional":false,"precision":32,"scale":0},
      {"name":"b","minor":"VARCHAR","optional":true,"precision":38,"scale":0},
      {"name":"t","minor":"TIMESTAMP","optional":false,"precision":26,"scale":6}])PLAN");
    auto original = protocol::Writer()
                        .integer(1, 24)
                        .integer(2, 0)
                        .integer(3, 8192)
                        .integer(4, 38)
                        .integer(5, 0)
                        .take();
    fields[1]["type"] = folly::base64Encode(original);
    auto schema = columnarType(fields);
    VELOX_CHECK(fieldsFromType(schema) == fields);
    auto definition = schemaDefinition(fields);
    auto header = batchHeaderFromDefinition(definition);
    auto received = fieldsFromType(columnarType(header["fields"]));
    VELOX_CHECK_EQ(received[0]["precision"].asInt(), 32);
    VELOX_CHECK_EQ(received[2]["precision"].asInt(), 26);
    VELOX_CHECK_EQ(received[2]["scale"].asInt(), 6);
    auto raw = folly::base64Decode(received[1]["type"].asString());
    VELOX_CHECK_EQ(protocol::Message(raw).integer(3), 8192);
    VELOX_CHECK(!received[2]["optional"].asBool());
    // Scalar Type instances are shared by Velox; metadata must stay per field.
    auto another = fields;
    another[0]["optional"] = true;
    another[0]["precision"] = 0;
    auto second = columnarType(another);
    VELOX_CHECK(!fieldsFromType(schema)[0]["optional"].asBool());
    VELOX_CHECK(fieldsFromType(second)[0]["optional"].asBool());
    FragmentPlanConverter converter(
        leaf.get(),
        [&](const folly::dynamic &node) {
          auto input = fields;
          if (node["@id"].asInt() == 9)
            for (auto &f : input)
              f["name"] = "r_" + f["name"].asString();
          return SourceBinding{
              columnarType(input), SourceKind::JniScan,
              [](memory::MemoryPool *) -> std::shared_ptr<BatchSource> {
                return {};
              }};
        },
        [](const folly::dynamic &, const RowTypePtr &) {
          return SinkFactory{};
        });
    auto project =
        converter.convert(folly::parseJson(R"PLAN({"pop":"project","@id":1,
      "exprs":[{"ref":"`x`","expr":"`a`"},{"ref":"`y`","expr":"`b`"},
               {"ref":"`z`","expr":"cast(`a` as BIGINT)"},
               {"ref":"`k`","expr":"if(`b` == 'x', 0, 1)"}],
      "child":{"pop":"test-scan","@id":2}})PLAN"));
    auto p = fieldsFromType(project->outputType());
    VELOX_CHECK(!p[0]["optional"].asBool());
    VELOX_CHECK_EQ(p[0]["precision"].asInt(), 32);
    VELOX_CHECK(p[1]["optional"].asBool());
    VELOX_CHECK_EQ(p[1]["precision"].asInt(), 38);
    VELOX_CHECK(!p[2]["optional"].asBool());
    VELOX_CHECK(!p[3]["optional"].asBool());
    auto aggregate = converter.convert(
        folly::parseJson(R"PLAN({"pop":"hash-aggregate","@id":3,
      "keys":[{"ref":"`k`","expr":"if(`a` < 0, 0, 1)"}],
      "exprs":[{"ref":"`n`","expr":"count(`a`)"},{"ref":"`s`","expr":"sum(`a`)"}],
      "child":{"pop":"test-scan","@id":4}})PLAN"));
    auto a = fieldsFromType(aggregate->outputType());
    VELOX_CHECK(!a[0]["optional"].asBool());
    VELOX_CHECK(!a[1]["optional"].asBool());
    VELOX_CHECK(a[2]["optional"].asBool());
    auto join =
        converter.convert(folly::parseJson(R"PLAN({"pop":"hash-join","@id":5,
      "joinType":"LEFT","conditions":[{"relationship":"==","left":"`a`","right":"`r_a`"}],
      "left":{"pop":"test-scan","@id":8},"right":{"pop":"test-scan","@id":9}})PLAN"));
    auto j = fieldsFromType(join->outputType());
    VELOX_CHECK(j[0]["optional"].asBool()); // build/right widened by LEFT join
    VELOX_CHECK(!j[3]["optional"].asBool());
    VELOX_CHECK_EQ(j[0]["precision"].asInt(), 32);
    // The required scalar uses one value buffer, including after schema RPC.
    auto vector =
        BaseVector::create<FlatVector<int32_t>>(INTEGER(), 2, leaf.get());
    vector->set(0, 7);
    vector->set(1, 9);
    auto input =
        std::make_shared<RowVector>(leaf.get(), ROW({"a"}, {INTEGER()}),
                                    nullptr, 2, std::vector<VectorPtr>{vector});
    auto wire = encodeBatch(input, folly::dynamic::array(fields[0]));
    auto wireHeader = batchHeaderFromDefinition(batchDefinition(wire));
    VELOX_CHECK_EQ(wireHeader["fields"][0]["lengths"].size(), 1);
    VELOX_CHECK_EQ(wireHeader["fields"][0]["precision"].asInt(), 32);
    auto decoded = decodeBatch({wireHeader, wire.data, {}}, leaf.get());
    VELOX_CHECK_EQ(decoded->childAt(0)->as<FlatVector<int32_t>>()->valueAt(1),
                   9);
    vector->setNull(0, true);
    bool rejected = false;
    try {
      encodeBatch(input, folly::dynamic::array(fields[0]));
    } catch (const std::exception &) {
      rejected = true;
    }
    VELOX_CHECK(rejected, "Required output cannot silently encode NULL");
    std::cout << "Drill schema metadata, computed nullability, outer join and "
                 "wire layouts passed.\n";
    return 0;
  } catch (const std::exception &error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
