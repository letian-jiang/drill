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
#include "execution/VeloxRuntime.h"
#include "scan/iceberg/IcebergArrowBatch.h"
#include <cstring>
#include <iostream>
#include <velox/vector/DecodedVector.h>
using namespace drill::nativeexec;
namespace {
struct Node {
  std::string name, format;
  ArrowSchema schema{};
  ArrowArray array{};
  uint8_t valid = 0xff;
  std::vector<int64_t> values;
  std::vector<int32_t> offsets;
  std::string bytes;
  std::vector<const void *> buffers;
  std::vector<std::unique_ptr<Node>> children;
  std::unique_ptr<Node> dictionary;
  std::vector<ArrowSchema *> schemas;
  std::vector<ArrowArray *> arrays;
  Node(std::string n, std::string f, int count)
      : name(std::move(n)), format(std::move(f)) {
    array.length = count;
  }
  void bind() {
    if (dictionary) {
      dictionary->bind();
      schema.dictionary = &dictionary->schema;
      array.dictionary = &dictionary->array;
    }
    for (const auto &child : children) {
      child->bind();
      schemas.push_back(&child->schema);
      arrays.push_back(&child->array);
    }
    schema.name = name.c_str();
    schema.format = format.c_str();
    schema.release = [](ArrowSchema *s) { s->release = nullptr; };
    array.release = [](ArrowArray *a) { a->release = nullptr; };
    schema.n_children = array.n_children = children.size();
    schema.children = schemas.data();
    array.children = arrays.data();
    array.buffers = buffers.data();
    array.n_buffers = buffers.size();
  }
  void unchanged() const {
    VELOX_CHECK_EQ(std::string(schema.format), format);
    VELOX_CHECK(schema.release && array.release,
                "An original child was consumed by a view");
    for (const auto &child : children)
      child->unchanged();
    if (dictionary)
      dictionary->unchanged();
  }
};
struct Tree {
  std::unique_ptr<Node> root;
  int &schemas, &arrays;
};
using Owner = std::shared_ptr<Tree>;
Owner lease(std::unique_ptr<Node> root, int &schemas, int &arrays) {
  root->bind();
  auto owner = std::make_shared<Tree>(Tree{std::move(root), schemas, arrays});
  owner->root->schema.private_data = new Owner(owner);
  owner->root->schema.release = [](ArrowSchema *s) {
    auto *lease = static_cast<Owner *>(s->private_data);
    for (const auto &child : (*lease)->root->children)
      child->unchanged();
    ++(*lease)->schemas;
    delete lease;
    s->release = nullptr;
  };
  owner->root->array.private_data = new Owner(owner);
  owner->root->array.release = [](ArrowArray *a) {
    auto *lease = static_cast<Owner *>(a->private_data);
    for (const auto &child : (*lease)->root->children)
      child->unchanged();
    ++(*lease)->arrays;
    delete lease;
    a->release = nullptr;
  };
  return owner;
}
Owner fixture(int &schemas, int &arrays, bool large, bool malformed,
              bool dictionary = false) {
  auto root = std::make_unique<Node>("", "+s", 3);
  root->buffers = {nullptr};
  auto list = std::make_unique<Node>("items", large ? "+L" : "+l", 3);
  list->valid = 0x5;
  list->array.null_count = 1;
  if (large) {
    list->values = {0, 2, 3, malformed ? INT64_MAX : 4};
    list->buffers = {&list->valid, list->values.data()};
  } else {
    list->offsets = {0, 2, 3, malformed ? INT32_MAX : 4};
    list->buffers = {&list->valid, list->offsets.data()};
  }
  auto members = std::make_unique<Node>("element", "+s", 4);
  members->valid = 0xd;
  members->array.null_count = 1;
  members->buffers = {&members->valid};
  auto ts = std::make_unique<Node>("ts", "tsu:UTC", 4);
  ts->values = {-1001, -1, 1001, INT64_MIN};
  ts->buffers = {nullptr, ts->values.data()};
  auto time = std::make_unique<Node>("t", "ttu", 4);
  time->values = {1000999, 1001001, 86399999999LL, 0};
  time->buffers = {nullptr, time->values.data()};
  auto text = std::make_unique<Node>("text", "u", 4);
  text->bytes = std::string(1000, 'a') + std::string("\0\xff", 2) +
                std::string(1000, 'z');
  text->offsets = {0, 1000, 1001, 1002, 2002};
  text->buffers = {nullptr, text->offsets.data(), text->bytes.data()};
  if (dictionary) {
    text->format = "i";
    text->offsets = {0, 1, 0, 1};
    text->buffers = {nullptr, text->offsets.data()};
    text->dictionary = std::make_unique<Node>("", "u", 2);
    auto &values = *text->dictionary;
    values.bytes = "onetwo";
    values.offsets = {0, 3, 6};
    values.buffers = {nullptr, values.offsets.data(), values.bytes.data()};
  }
  auto binary = std::make_unique<Node>("fixed", "w:4", 4);
  binary->bytes = std::string("\0\xff\x80\1", 4) + std::string(12, 'x');
  binary->buffers = {nullptr, binary->bytes.data()};
  members->children.push_back(std::move(ts));
  members->children.push_back(std::move(time));
  members->children.push_back(std::move(text));
  members->children.push_back(std::move(binary));
  list->children.push_back(std::move(members));
  root->children.push_back(std::move(list));
  return lease(std::move(root), schemas, arrays);
}
void mapTest(memory::MemoryPool *pool, bool malformed) {
  int schemas = 0, arrays = 0;
  auto root = std::make_unique<Node>("", "+s", 3);
  root->buffers = {nullptr};
  auto dict = std::make_unique<Node>("dict", "+m", 3);
  dict->valid = 0x5;
  dict->array.null_count = 1;
  dict->offsets = {0, 2, 3, malformed ? INT32_MAX : 4};
  dict->buffers = {&dict->valid, dict->offsets.data()};
  auto entries = std::make_unique<Node>("entries", "+s", 4);
  entries->buffers = {nullptr};
  auto keys = std::make_unique<Node>("key", "l", 4);
  keys->values = {7, 7, 9, 42};
  keys->buffers = {nullptr, keys->values.data()};
  auto values = std::make_unique<Node>("value", "+s", 4);
  values->valid = 0xd;
  values->array.null_count = 1;
  values->buffers = {&values->valid};
  auto ts = std::make_unique<Node>("ts", "tsu:", 4);
  ts->values = {-1001, -1, 1001, 1924992000123456};
  ts->buffers = {nullptr, ts->values.data()};
  auto text = std::make_unique<Node>("txt", "u", 4);
  text->bytes = std::string(1000, 'a') + std::string(1000, 'z');
  text->offsets = {0, 1000, 1000, 1000, 2000};
  text->buffers = {nullptr, text->offsets.data(), text->bytes.data()};
  values->children.push_back(std::move(ts));
  values->children.push_back(std::move(text));
  entries->children.push_back(std::move(keys));
  entries->children.push_back(std::move(values));
  dict->children.push_back(std::move(entries));
  root->children.push_back(std::move(dict));
  auto original = lease(std::move(root), schemas, arrays);
  auto schema = original->root->schema;
  auto array = original->root->array;
  RowVectorPtr rows;
  try {
    rows = importIcebergBatch(schema, array, pool);
  } catch (const VeloxException &) {
    if (!malformed)
      throw;
  }
  VELOX_CHECK(!schema.release && !array.release);
  if (malformed) {
    VELOX_CHECK(!rows && schemas == 1 && arrays == 1);
    return;
  }
  auto *map = rows->childAt(0)->as<MapVector>();
  VELOX_CHECK(map->isNullAt(1));
  VELOX_CHECK_EQ(map->offsetAt(2), 3);
  VELOX_CHECK_EQ(map->sizeAt(0), 2);
  auto *members = map->mapValues()->as<RowVector>();
  VELOX_CHECK(members->isNullAt(1));
  {
    DecodedVector keys(*map->mapKeys()), ts(*members->childAt(0));
    VELOX_CHECK_EQ(keys.valueAt<int64_t>(0), 7);
    VELOX_CHECK_EQ(keys.valueAt<int64_t>(1), 7);
    VELOX_CHECK(ts.valueAt<Timestamp>(0) == Timestamp(-1, 998999000));
  }
  auto retained = members->childAt(1);
  rows.reset();
  original.reset();
  VELOX_CHECK_EQ(schemas, 0);
  VELOX_CHECK_EQ(arrays, 0);
  {
    DecodedVector text(*retained);
    VELOX_CHECK_EQ(text.valueAt<StringView>(3).str(), std::string(1000, 'z'));
  }
  retained.reset();
  VELOX_CHECK_EQ(schemas, 1);
  VELOX_CHECK_EQ(arrays, 1);
}
void test(memory::MemoryPool *pool, bool large, bool malformed) {
  int schemas = 0, arrays = 0;
  auto original = fixture(schemas, arrays, large, malformed);
  ArrowSchema schema = original->root->schema;
  ArrowArray array = original->root->array;
  RowVectorPtr rows;
  try {
    rows = importIcebergBatch(schema, array, pool);
  } catch (const VeloxException &) {
    if (!malformed)
      throw;
  }
  VELOX_CHECK(!schema.release && !array.release);
  if (malformed) {
    VELOX_CHECK(!rows && schemas == 1 && arrays == 1);
    return;
  }
  auto *list = rows->childAt(0)->as<ArrayVector>();
  VELOX_CHECK(list->size() == 3 && list->isNullAt(1));
  VELOX_CHECK_EQ(list->offsetAt(2), 3);
  VELOX_CHECK_EQ(list->sizeAt(2), 1);
  auto *members = list->elements()->as<RowVector>();
  VELOX_CHECK(members->isNullAt(1));
  auto retained = members->childAt(2);
  {
    DecodedVector ts(*members->childAt(0)), time(*members->childAt(1)),
        binary(*members->childAt(3));
    VELOX_CHECK(ts.valueAt<Timestamp>(0) == Timestamp(-1, 998999000));
    VELOX_CHECK(ts.valueAt<Timestamp>(3) ==
                Timestamp(-9223372036855LL, 224192000));
    VELOX_CHECK(members->childAt(1)->type()->equivalent(*TIME_MICRO_UTC()));
    VELOX_CHECK_EQ(time.valueAt<int64_t>(2), 86399999999LL);
    VELOX_CHECK_EQ(binary.valueAt<StringView>(0).str(),
                   std::string("\0\xff\x80\1", 4));
  }
  rows.reset();
  original.reset();
  VELOX_CHECK_EQ(schemas, 0);
  VELOX_CHECK_EQ(arrays, 0);
  {
    DecodedVector text(*retained);
    VELOX_CHECK_EQ(text.valueAt<StringView>(3).str(), std::string(1000, 'z'));
  }
  retained.reset();
  VELOX_CHECK_EQ(schemas, 1);
  VELOX_CHECK_EQ(arrays, 1);
}
} // namespace
int main() {
  try {
    VeloxRuntime runtime(2);
    auto root = memory::memoryManager()->addRootPool();
    auto pool = root->addLeafChild("nested-arrow");
    for (bool large : {false, true}) {
      test(pool.get(), large, false);
      test(pool.get(), large, true);
    }
    mapTest(pool.get(), false);
    mapTest(pool.get(), true);
    int schemas = 0, arrays = 0;
    auto original = fixture(schemas, arrays, false, false, true);
    auto schema = original->root->schema;
    auto array = original->root->array;
    bool rejected = false;
    try {
      importIcebergBatch(schema, array, pool.get());
    } catch (const VeloxException &error) {
      rejected = std::string(error.what()).find("dictionary encoding") !=
                 std::string::npos;
    }
    VELOX_CHECK(rejected);
    VELOX_CHECK(!schema.release && !array.release);
    VELOX_CHECK_EQ(schemas, 1);
    VELOX_CHECK_EQ(arrays, 1);
    VELOX_CHECK_EQ(pool->usedBytes(), 0);
    std::cout << "Nested Iceberg Arrow: list/large_list, struct NULL masks, "
                 "recursive TIME/TIMESTAMP/fixed binary, retained grandchild, "
                 "original release tree, invalid offsets, dictionary rejection "
                 "and no leaks passed\n";
    return 0;
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
