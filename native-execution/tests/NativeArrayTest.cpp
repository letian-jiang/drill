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
#include "exchange/ReceiverInbox.h"
#include "exchange/SenderBuffer.h"
#include "plan/FragmentPlanConverter.h"
#include <atomic>
#include <cstring>
#include <iostream>
#include <velox/vector/DecodedVector.h>
#include <velox/vector/FlatVector.h>
using namespace drill::nativeexec;
namespace {
class ArrayInput final : public BatchSource {
 public:
  ArrayInput(RowVectorPtr rows, std::shared_ptr<std::atomic<bool>> claimed)
      : rows_(std::move(rows)), claimed_(std::move(claimed)) {}
  std::optional<RowVectorPtr> next(ContinueFuture &) override {
    if (claimed_->exchange(true)) return std::nullopt;
    return rows_;
  }
  void cancel() override {}
 private:
  RowVectorPtr rows_;
  std::shared_ptr<std::atomic<bool>> claimed_;
};
folly::dynamic schema() {
  return folly::parseJson(R"([
    {"name":"r","minor":"BIGINT","optional":false,"repeated":true,
     "element":{"name":"$data$","minor":"BIGINT","optional":false}},
    {"name":"s","minor":"LIST","optional":true,
     "element":{"name":"$data$","minor":"VARCHAR","optional":true}},
    {"name":"maps","minor":"MAP","optional":false,"repeated":true,
     "element":{"name":"$data$","minor":"MAP","optional":false,"children":[
       {"name":"n","minor":"BIGINT","optional":true},
       {"name":"tags","minor":"VARCHAR","optional":false,"repeated":true,
        "element":{"name":"$data$","minor":"VARCHAR","optional":false}}]}},
    {"name":"nested","minor":"LIST","optional":false,"repeated":true,
     "element":{"name":"$data$","minor":"INT","optional":false,"repeated":true,
       "element":{"name":"$data$","minor":"INT","optional":false}}},
    {"name":"empty","minor":"MAP","optional":false,"repeated":true,
     "element":{"name":"$data$","minor":"MAP","optional":false,"children":[]}}
  ])");
}
VectorPtr wrap(VectorPtr value, int encoding, memory::MemoryPool *pool) {
  if (encoding == 2) return BaseVector::wrapInConstant(5, 2, value);
  if (!encoding) return value;
  std::vector<vector_size_t> mapping{3,2,0,1,2};
  auto indices = AlignedBuffer::allocate<vector_size_t>(mapping.size(), pool);
  std::memcpy(indices->asMutable<vector_size_t>(), mapping.data(), mapping.size()*4);
  return BaseVector::wrapInDictionary(nullptr, indices, mapping.size(), value);
}
VectorPtr array(TypePtr type, VectorPtr elements, memory::MemoryPool *pool,
                std::vector<vector_size_t> offsets, std::vector<vector_size_t> sizes) {
  auto o = AlignedBuffer::allocate<vector_size_t>(offsets.size(), pool);
  auto s = AlignedBuffer::allocate<vector_size_t>(sizes.size(), pool);
  std::memcpy(o->asMutable<vector_size_t>(), offsets.data(), offsets.size()*4);
  std::memcpy(s->asMutable<vector_size_t>(), sizes.data(), sizes.size()*4);
  return std::make_shared<ArrayVector>(pool, type, nullptr, offsets.size(), o, s, std::move(elements));
}
RowVectorPtr input(memory::MemoryPool *pool, int encoding) {
  auto type = columnarType(schema());
  auto row = BaseVector::create<RowVector>(type, 4, pool);
  auto numbers = BaseVector::create(BIGINT(), 9, pool);
  for (int i=0;i<9;++i) numbers->as<FlatVector<int64_t>>()->set(i, i*11);
  numbers->setNull(0, true); // Required but not referenced by any array.
  row->childAt(0) = array(type->childAt(0), numbers, pool, {1,5,7,8}, {4,2,1,0});
  auto strings = BaseVector::create(VARCHAR(), 6, pool);
  for (int i=0;i<6;++i) {
    std::string text(2000+i, 'a'+i); text += "雪🚀"; text.push_back('\0');
    strings->as<FlatVector<StringView>>()->set(i, StringView(text));
  }
  strings->setNull(2, true);
  row->childAt(1) = array(type->childAt(1), wrap(strings, encoding, pool), pool, {0,1,2,4}, {1,1,2,1});
  // NULL arrays may have nonempty backing ranges; those elements are invisible.
  row->childAt(1)->setNull(1, true);
  auto maps = BaseVector::create<RowVector>(type->childAt(2)->childAt(0), 6, pool);
  for (int i=0;i<6;++i) maps->childAt(0)->as<FlatVector<int64_t>>()->set(i, 100+i);
  maps->childAt(0)->setNull(2,true);
  auto tags = BaseVector::create(VARCHAR(), 8, pool);
  for (int i=0;i<8;++i) {
    std::string text(1024+i, 'x'+i);
    tags->as<FlatVector<StringView>>()->set(i, StringView(text));
  }
  maps->childAt(1) = array(maps->type()->childAt(1), tags, pool, {0,2,2,4,5,6}, {2,0,2,1,1,2});
  auto mapElements = encoding ? wrap(maps,encoding,pool) : maps;
  row->childAt(2) = array(type->childAt(2), mapElements, pool, {0,2,2,4}, {2,0,2,1});
  auto ints = BaseVector::create(INTEGER(), 8, pool);
  for (int i=0;i<8;++i) ints->as<FlatVector<int32_t>>()->set(i,i);
  auto inner = array(type->childAt(3)->childAt(0), ints, pool, {0,2,3,3,6}, {2,1,0,3,2});
  row->childAt(3) = array(type->childAt(3), inner, pool, {0,1,3,4}, {1,2,1,1});
  auto empty = BaseVector::create(ROW({},{}), 5, pool);
  row->childAt(4) = array(type->childAt(4), empty, pool, {0,1,3,3}, {1,2,0,2});
  if (encoding) {
    auto out = BaseVector::create<RowVector>(type,5,pool);
    for (int i=0;i<5;++i) out->childAt(i)=wrap(row->childAt(i),encoding,pool);
    return out;
  }
  return row;
}
std::string value(const BaseVector *v, vector_size_t row) {
  DecodedVector d(*v);
  if(d.isNullAt(row)) return "null";
  auto index=d.index(row);
  if(v->type()->isRow()) {
    std::string out="{";
    for(const auto &c:d.base()->as<RowVector>()->children()) {auto s=value(c.get(),index);out+=std::to_string(s.size())+":"+s;}
    return out+"}";
  }
  if(v->type()->isArray()) {
    auto *a=d.base()->as<ArrayVector>();std::string out="[";
    for(int64_t i=a->offsetAt(index);i<int64_t(a->offsetAt(index))+a->sizeAt(index);++i) {
      auto s=value(a->elements().get(),i);out+=std::to_string(s.size())+":"+s;
    }
    return out+"]";
  }
  return d.base()->toString(index);
}
void buffers(const BaseVector *v, std::vector<const void *> &out, memory::MemoryPool *pool=nullptr) {
  if(pool) VELOX_CHECK(v->pool()==pool);
  if(v->type()->isRow()) for(const auto &c:v->as<RowVector>()->children()) buffers(c.get(),out,pool);
  else if(v->type()->isArray()) {
    auto *a=v->as<ArrayVector>();out.push_back(a->offsets()->as<char>());out.push_back(a->sizes()->as<char>());
    buffers(a->elements().get(),out,pool);
  } else out.push_back(v->values()?v->values()->as<char>():nullptr);
}
void test(int encoding,bool selected,int route) {
  auto receiving=memory::memoryManager()->addRootPool();auto pool=receiving->addLeafChild("receive");
  auto inbox=std::make_shared<ReceiverInbox>();inbox->senders({7});
  RowVectorPtr result;std::vector<std::string> expected;std::vector<const void *> addresses;
  {
    auto sourceRoot=memory::memoryManager()->addRootPool();auto sourcePool=sourceRoot->addLeafChild("source");
    auto source=input(sourcePool.get(),encoding);auto fields=fieldsFromType(source->rowType());
    VELOX_CHECK(fields[0]["minor"]=="BIGINT" && fields[0]["repeated"].asBool());
    VELOX_CHECK(fields[1]["minor"]=="LIST" && fields[2]["minor"]=="MAP" && fields[2]["repeated"].asBool());
    std::vector<vector_size_t> rows{3,0,3,2,1};
    const vector_size_t count=selected?rows.size():source->size();
    for(int i=0;i<count;++i) expected.push_back(value(source.get(),selected?rows[i]:i));
    if(route==0) {
      auto wire=encodeBatch(source,fields,selected?&rows:nullptr);
      auto header=batchHeaderFromDefinition(batchDefinition(wire));
      result=decodeBatch({header,wire.data,{}},pool.get());
      std::vector<drill::nativeexec::BufferView> views;size_t offset=0;
      for(const auto &f:header["fields"])for(const auto &n:f["lengths"]){views.push_back({wire.data.data()+offset,size_t(n.asInt())});offset+=n.asInt();}
      auto cached=decodeBatchBuffers(columnarLayout(fields),count,views,pool.get());
      for(int i=0;i<count;++i) VELOX_CHECK(result->equalValueAt(cached.get(),i,i));
      std::fill(wire.data.begin(),wire.data.end(),0);
    } else if(route==1)result=copyLocalBatch(source,source->rowType(),fields,pool.get(),selected?&rows:nullptr);
    else if(route==2) {
      auto snapshot=selected?copyLocalBatch(source,source->rowType(),fields,sourcePool.get(),&rows):source;
      inbox->pushLocal(snapshot,fields,7);
    } else {
      auto senderPool=sourceRoot->addLeafChild("sender");
      auto buffer=route==4?inbox->senderBuffer(source->rowType(),32):std::make_shared<SenderBuffer>(source->rowType(),senderPool.get(),32);
      VELOX_CHECK(buffer->append(source,selected?&rows:nullptr).empty());auto snapshot=buffer->flush();
      buffers(snapshot.get(),addresses);
      VELOX_CHECK(inbox->pushOwnedLocal(std::move(snapshot),fields,7));
    }
  }
  if(route>=2){inbox->end(7);auto reader=inbox->factory()(pool.get());ContinueFuture f;auto next=reader->next(f);VELOX_CHECK(next&&*next);result=std::move(*next);VELOX_CHECK(!reader->next(f));reader.reset();inbox.reset();}
  std::vector<const void *> after;buffers(result.get(),after,pool.get());
  if(route>=3)VELOX_CHECK(after==addresses,"Array owned handoff copied offsets/sizes/values");
  for(int i=0;i<result->size();++i) VELOX_CHECK(value(result.get(),i)==expected[i],"Array mismatch encoding={} selected={} route={} row={}",encoding,selected,route,i);
  auto retained=result->childAt(1)->as<ArrayVector>()->elements();result.reset();
  VELOX_CHECK(retained->pool()==pool.get());retained.reset();VELOX_CHECK_EQ(pool->usedBytes(),0);
}
void largeAndRejected(memory::MemoryPool *pool) {
  auto fields=folly::dynamic::array(schema()[0]);auto type=columnarType(fields);
  auto elements=BaseVector::create(BIGINT(),70003,pool);
  for(int i=0;i<70003;++i)elements->as<FlatVector<int64_t>>()->set(i,i);
  auto row=BaseVector::create<RowVector>(type,2,pool);
  row->childAt(0)=array(type->childAt(0),elements,pool,{0,70000},{70000,3});
  auto wire=encodeBatch(row,fields);auto header=batchHeaderFromDefinition(batchDefinition(wire));
  auto decoded=decodeBatch({header,wire.data,{}},pool);
  VELOX_CHECK(row->equalValueAt(decoded.get(),0,0) && row->equalValueAt(decoded.get(),1,1));
  elements->setNull(0,true);bool rejected=false;
  try{encodeBatch(row,fields);}catch(const VeloxException&){rejected=true;}VELOX_CHECK(rejected);
  auto schemaHeader=batchHeaderFromDefinition(schemaDefinition(schema()));
  VELOX_CHECK(columnarType(schemaHeader["fields"])->equivalent(*columnarType(schema())));
}
void expressions(VeloxRuntime &runtime) {
  auto root=memory::memoryManager()->addRootPool();auto pool=root->addLeafChild("input");
  auto data=input(pool.get(),0);auto claimed=std::make_shared<std::atomic<bool>>(false);
  int count=0;
  FragmentPlanConverter converter(pool.get(),[&](const folly::dynamic &){
    return SourceBinding{data->rowType(),SourceKind::Receiver,[data,claimed](memory::MemoryPool *){
      return std::make_shared<ArrayInput>(data,claimed);
    }};
  },[&](const folly::dynamic &,const RowTypePtr &){return SinkFactory([&](memory::MemoryPool *){
    return BatchSink([&](RowVectorPtr batch)->ContinueFuture{
      if(!batch)return {};
      DecodedVector first(*batch->childAt(0)),negative(*batch->childAt(1)),size(*batch->childAt(2)),
          map(*batch->childAt(3)),nested(*batch->childAt(4)),strings(*batch->childAt(5)),outside(*batch->childAt(6));
      const int64_t numbers[]{11,55,77,0},mapValues[]{100,0,0,104};
      const int32_t sizes[]{4,2,1,0},nestedValues[]{0,2,3,6};
      for(int i=0;i<batch->size();++i,++count){
        VELOX_CHECK(count<4);VELOX_CHECK(negative.isNullAt(i) && outside.isNullAt(i));
        VELOX_CHECK_EQ(size.valueAt<int32_t>(i),sizes[count]);
        VELOX_CHECK(count==3?first.isNullAt(i):first.valueAt<int64_t>(i)==numbers[count]);
        VELOX_CHECK(count==1||count==2?map.isNullAt(i):map.valueAt<int64_t>(i)==mapValues[count]);
        VELOX_CHECK_EQ(nested.valueAt<int32_t>(i),nestedValues[count]);
        VELOX_CHECK(count==1||count==2?strings.isNullAt(i):strings.valueAt<StringView>(i).str().ends_with(std::string("雪🚀\0",8)));
      }
      return {};
    });
  });});
  auto plan=converter.convert(folly::parseJson(R"PLAN({"pop":"single-sender","@id":0,"child":{
    "pop":"project","@id":1,"exprs":[
      {"ref":"`first`","expr":"`r`[0]"},{"ref":"`negative`","expr":"`r`[-1]"},
      {"ref":"`size`","expr":"repeated_count(`r`)"},{"ref":"`map`","expr":"`maps`[0].`n`"},
      {"ref":"`nested`","expr":"`nested`[0][0]"},{"ref":"`text`","expr":"`s`[0]"},
      {"ref":"`outside`","expr":"`r`[999]"}],"child":{"pop":"unordered-receiver","@id":2}}})PLAN"));
  auto task=runtime.task("array-index-semantics",plan,root);auto complete=task->taskCompletionFuture();task->start(4);
  std::move(complete).get();auto error=task->error();auto deleted=task->taskDeletionFuture();
  task.reset();std::move(deleted).get();if(error)std::rethrow_exception(error);VELOX_CHECK_EQ(count,4);
}
void flatten(VeloxRuntime &runtime) {
  auto root=memory::memoryManager()->addRootPool();auto pool=root->addLeafChild("flatten-input");
  auto fields=folly::dynamic::array(schema()[0]);auto type=columnarType(fields);
  auto elements=BaseVector::create(BIGINT(),70003,pool.get());
  for(int i=0;i<70003;++i)elements->as<FlatVector<int64_t>>()->set(i,i);
  auto data=BaseVector::create<RowVector>(type,2,pool.get());
  data->childAt(0)=array(type->childAt(0),elements,pool.get(),{0,70000},{70000,3});
  auto claimed=std::make_shared<std::atomic<bool>>(false);int64_t count=0;int batches=0;
  FragmentPlanConverter converter(pool.get(),[&](const folly::dynamic &){
    return SourceBinding{type,SourceKind::Receiver,[data,claimed](memory::MemoryPool *){
      return std::make_shared<ArrayInput>(data,claimed);
    }};
  },[&](const folly::dynamic &,const RowTypePtr &){return SinkFactory([&](memory::MemoryPool *){
    return BatchSink([&](RowVectorPtr batch)->ContinueFuture{
      if(!batch)return {};
      VELOX_CHECK_LE(batch->size(),65535,"Flatten must split large output before Drill exchange");
      DecodedVector values(*batch->childAt(0));
      for(int i=0;i<batch->size();++i)VELOX_CHECK_EQ(values.valueAt<int64_t>(i),count++);
      auto wire=encodeBatch(batch,fieldsFromType(batch->rowType()));
      auto copy=decodeBatch(wire,pool.get());
      for(int i=0;i<batch->size();++i)VELOX_CHECK(batch->equalValueAt(copy.get(),i,i));
      ++batches;return {};
    });
  });});
  auto plan=converter.convert(folly::parseJson(R"({"pop":"single-sender","@id":0,"child":{
    "pop":"flatten","@id":1,"column":"`r`","child":{"pop":"unordered-receiver","@id":2}}})"));
  auto task=runtime.task("flatten-large-output",plan,root);auto complete=task->taskCompletionFuture();task->start(4);
  std::move(complete).get();auto error=task->error();auto deleted=task->taskDeletionFuture();
  task.reset();std::move(deleted).get();if(error)std::rethrow_exception(error);
  VELOX_CHECK_EQ(count,70003);VELOX_CHECK_GT(batches,1);
}
}
int main(){try{VeloxRuntime runtime(4);for(int e=0;e<3;++e)for(bool s:{false,true})for(int r=0;r<5;++r)test(e,s,r);
  expressions(runtime);
  flatten(runtime);
  auto root=memory::memoryManager()->addRootPool();auto pool=root->addLeafChild("checks");largeAndRejected(pool.get());VELOX_CHECK_EQ(pool->usedBytes(),0);
  std::cout<<"LIST/REPEATED scalar/MAP/nested arrays: recursive wire/cache, NULL/empty, encodings/selection, owned handoff and large element counts passed\n";return 0;
}catch(const std::exception&e){std::cerr<<e.what()<<'\n';return 1;}}
