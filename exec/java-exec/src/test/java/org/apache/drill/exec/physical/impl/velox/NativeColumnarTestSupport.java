/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.physical.impl.velox;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.DrillBuf;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared.RecordBatchDef;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;

import static org.apache.drill.exec.physical.impl.velox.NativeColumnarBridge.fields;
import static org.apache.drill.exec.physical.impl.velox.NativeColumnarBridge.isArray;
import static org.apache.drill.exec.physical.impl.velox.NativeColumnarBridge.validateType;

/** Test fixtures for round trips of the native columnar wire representation. */
final class NativeColumnarTestSupport {
  static final ObjectMapper JSON = new ObjectMapper();

  private NativeColumnarTestSupport() {
  }

  static List<MaterializedField> materialized(JsonNode fields) {
    List<MaterializedField> result = new ArrayList<>();
    for (JsonNode f : fields) {
      MajorType type;
      try {
        if (f.has("type")) {
          type = MajorType.parseFrom(java.util.Base64.getDecoder().decode(f.get("type").asText()));
        } else {
          MajorType.Builder builder = MajorType.newBuilder().setMinorType(MinorType.valueOf(f.get("minor").asText())).setMode(f.get("optional").asBoolean() ? DataMode.OPTIONAL : DataMode.REQUIRED);
          if (f.path("repeated").asBoolean()) {
            builder.setMode(DataMode.REPEATED);
          }
          if (f.has("precision")) {
            builder.setPrecision(f.get("precision").asInt());
          }
          if (f.has("scale")) {
            builder.setScale(f.get("scale").asInt());
          }
          type = builder.build();
        }
      } catch (java.io.IOException e) {
        throw new IllegalStateException(e);
      }
      validateType(type);
      MaterializedField field = MaterializedField.create(f.get("name").asText(), type);
      if (isArray(field)) {
        var element = materialized(JSON.createArrayNode().add(f.get("element"))).get(0);
        if (type.getMinorType() == MinorType.MAP) {
          for (var child : element.getChildren()) {
            field.addChild(child);
          }
        } else if (type.getMinorType() == MinorType.LIST) {
          field.addChild(element);
        } else if (type.getMinorType() == MinorType.DICT) {
          var inner = MaterializedField.create("$inner$", element.getType());
          for (var child : element.getChildren()) {
            inner.addChild(child);
          }
          field.addChild(inner);
        }
      } else if (type.getMinorType() == MinorType.MAP || type.getMinorType() == MinorType.DICT) {
        if (!f.has("children")) {
          throw new IllegalStateException("Missing native MAP children");
        }
        for (MaterializedField child : materialized(f.get("children"))) {
          field.addChild(child);
        }
      }
      result.add(field);
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  private static void attachLengths(Map<String, Object> field, List<? extends Number> lengths, int[] index) {
    int start = index[0];
    if (Boolean.TRUE.equals(field.get("repeated")) || "LIST".equals(field.get("minor"))) {
      index[0] += Boolean.TRUE.equals(field.get("repeated")) ? 1 : 2;
      attachLengths((Map<String, Object>) field.get("element"), lengths, index);
    } else if ("MAP".equals(field.get("minor")) || "DICT".equals(field.get("minor"))) {
      if ("DICT".equals(field.get("minor"))) {
        ++index[0];
      }
      for (Map<String, Object> child : (List<Map<String, Object>>) field.get("children")) {
        attachLengths(child, lengths, index);
      }
    } else {
      String minor = (String) field.get("minor");
      index[0] += ((Boolean) field.get("optional") ? 1 : 0) + (minor.equals("VARCHAR") || minor.equals("VARBINARY") || minor.equals("VARDECIMAL") ? 2 : 1);
    }
    if (index[0] > lengths.size()) {
      throw new IllegalStateException("Native buffer layout mismatch");
    }
    field.put("lengths", new ArrayList<>(lengths.subList(start, index[0])));
  }

  private static void attachLengths(Map<String, Object> field, List<? extends Number> lengths) {
    int[] index = { 0 };
    attachLengths(field, lengths, index);
    if (index[0] != lengths.size()) {
      throw new IllegalStateException("Trailing native buffers");
    }
  }

  /**
   * Serialize existing Drill vectors in bulk for columnar interoperability checks.
   */
  static String copyBatch(RecordBatch batch, ByteBuffer target) throws Exception {
    if (batch.getSchema().getSelectionVectorMode() != BatchSchema.SelectionVectorMode.NONE) {
      throw new UnsupportedOperationException("SELECTION_VECTOR_REQUIRES_REMOVER");
    }
    List<Map<String, Object>> fields = fields(batch.getSchema());
    int col = 0;
    target.clear();
    for (VectorWrapper<?> wrapper : batch) {
      List<Integer> lengths = new ArrayList<>();
      for (DrillBuf buffer : wrapper.getValueVector().getBuffers(false)) {
        int length = buffer.readableBytes();
        lengths.add(length);
        target.put(buffer.nioBuffer(buffer.readerIndex(), length));
      }
      attachLengths(fields.get(col++), lengths);
    }
    return JSON.writeValueAsString(Map.of("fields", fields, "rows", batch.getRecordCount()));
  }

  static boolean load(RecordBatchLoader loader, BufferAllocator allocator, JsonNode header, ByteBuffer bytes) {
    int rows = header.get("rows").asInt();
    RecordBatchDef.Builder def = RecordBatchDef.newBuilder().setRecordCount(rows);
    int size = 0;
    List<MaterializedField> fields = materialized(header.get("fields"));
    for (int col = 0; col < fields.size(); ++col) {
      List<Integer> lengths = new ArrayList<>();
      JsonNode f = header.get("fields").get(col);
      if (f.has("lengths")) {
        for (JsonNode n : f.get("lengths")) {
          lengths.add(n.asInt());
        }
      }
      def.addField(fieldMetadata(fields.get(col), f, rows, allocator, bytes, bytes.position() + size));
      for (int n : lengths) {
        size = Math.addExact(size, n);
      }
    }
    if (rows == 0 && size == 0) {
      return loader.load(def.build(), null);
    }
    if (size != bytes.remaining()) {
      throw new IllegalStateException("Native batch buffer size mismatch");
    }
    try (DrillBuf buffer = allocator.buffer(size)) {
      buffer.setBytes(0, bytes);
      buffer.writerIndex(size);
      return loader.load(def.build(), buffer);
    }
  }

  private static SerializedField fieldMetadata(MaterializedField field, JsonNode descriptor, int rows, BufferAllocator allocator, ByteBuffer bytes, int offset) {
    if (isArray(field)) {
      var lengths = descriptor.get("lengths");
      boolean repeated = field.getType().getMode() == DataMode.REPEATED;
      int offsetsLength = lengths.get(0).asInt();
      int elements = offsetsLength == 0 && rows == 0 ? 0 : bytes.duplicate().order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt(offset + rows * 4);
      if (elements < 0 || offsetsLength != (rows + 1) * 4 && !(rows == 0 && offsetsLength == 0)) {
        throw new IllegalStateException("Invalid native array offsets");
      }
      var out = field.getAsBuilder().clearChild().setValueCount(rows);
      var offsetType = MajorType.newBuilder().setMinorType(MinorType.UINT4).setMode(DataMode.REQUIRED).build();
      out.addChild(MaterializedField.create("$offsets$", offsetType).getAsBuilder().setValueCount(rows + 1).setBufferLength(offsetsLength));
      int prefixBytes = offsetsLength;
      if (!repeated) {
        int bitsLength = lengths.get(1).asInt();
        var bitsType = MajorType.newBuilder().setMinorType(MinorType.UINT1).setMode(DataMode.REQUIRED).build();
        out.addChild(MaterializedField.create("$bits$", bitsType).getAsBuilder().setValueCount(rows).setBufferLength(bitsLength));
        prefixBytes += bitsLength;
      }
      var elementDescriptor = descriptor.get("element");
      var elementField = materialized(JSON.createArrayNode().add(elementDescriptor)).get(0);
      var element = fieldMetadata(elementField, elementDescriptor, elements, allocator, bytes, offset + prefixBytes);
      if (repeated && field.getType().getMinorType() == MinorType.MAP) {
        out.addAllChild(element.getChildList());
      } else {
        out.addChild(element);
      }
      int total = 0;
      for (JsonNode length : lengths) {
        total = Math.addExact(total, length.asInt());
      }
      if (prefixBytes + element.getBufferLength() != total) {
        throw new IllegalStateException("Native array buffer size mismatch");
      }
      return out.setBufferLength(total).build();
    }
    if (field.getType().getMinorType() == MinorType.DICT) {
      var lengths = descriptor.get("lengths");
      int offsetsLength = lengths.get(0).asInt();
      if (offsetsLength != (rows + 1) * 4 && !(rows == 0 && offsetsLength == 0)) {
        throw new IllegalStateException("Invalid native DICT offsets");
      }
      int entries = offsetsLength == 0 ? 0 : bytes.duplicate().order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt(offset + rows * 4);
      if (entries < 0) {
        throw new IllegalStateException("Invalid native DICT entry count");
      }
      var out = field.getAsBuilder().clearChild().setValueCount(rows);
      var offsetType = MajorType.newBuilder().setMinorType(MinorType.UINT4).setMode(DataMode.REQUIRED).build();
      out.addChild(MaterializedField.create("$offsets$", offsetType).getAsBuilder().setValueCount(rows + 1).setBufferLength(offsetsLength));
      int total = offsetsLength;
      for (JsonNode child : descriptor.get("children")) {
        var childField = materialized(JSON.createArrayNode().add(child)).get(0);
        var metadata = fieldMetadata(childField, child, entries, allocator, bytes, offset + total);
        total = Math.addExact(total, metadata.getBufferLength());
        out.addChild(metadata);
      }
      int declared = 0;
      for (JsonNode length : lengths) {
        declared = Math.addExact(declared, length.asInt());
      }
      if (declared != total) {
        throw new IllegalStateException("Native DICT buffer size mismatch");
      }
      return out.setBufferLength(total).build();
    }
    if (field.getType().getMinorType() != MinorType.MAP) {
      List<Integer> lengths = new ArrayList<>();
      for (JsonNode length : descriptor.get("lengths")) {
        lengths.add(length.asInt());
      }
      try (ValueVector vector = TypeHelper.getNewVector(field, allocator)) {
        return metadata(vector.getMetadata(), field.getType().getMode() == DataMode.OPTIONAL, field.getType().getMinorType(), rows, lengths);
      }
    }
    var out = field.getAsBuilder().clearChild().setValueCount(rows);
    int total = 0;
    for (JsonNode child : descriptor.get("children")) {
      MaterializedField childField = materialized(JSON.createArrayNode().add(child)).get(0);
      SerializedField metadata = fieldMetadata(childField, child, rows, allocator, bytes, offset + total);
      total = Math.addExact(total, metadata.getBufferLength());
      out.addChild(metadata);
    }
    int declared = 0;
    for (JsonNode length : descriptor.get("lengths")) {
      declared = Math.addExact(declared, length.asInt());
    }
    if (declared != total) {
      throw new IllegalStateException("Native MAP buffer size mismatch");
    }
    return out.setBufferLength(total).build();
  }

  private static SerializedField metadata(SerializedField template, boolean optional, MinorType minor, int rows, List<Integer> lengths) {
    SerializedField.Builder out = template.toBuilder().clearChild().setValueCount(rows);
    if (lengths.isEmpty()) {
      return out.setBufferLength(0).build();
    }
    int total = lengths.stream().mapToInt(Integer::intValue).sum();
    out.setBufferLength(total);
    if (optional) {
      out.addChild(template.getChild(0).toBuilder().setValueCount(rows).setBufferLength(lengths.get(0)));
      out.addChild(metadata(template.getChild(1), false, minor, rows, lengths.subList(1, lengths.size())));
    } else if (minor == MinorType.VARCHAR || minor == MinorType.VARBINARY || minor == MinorType.VARDECIMAL) {
      out.addChild(template.getChild(0).toBuilder().setValueCount(rows + 1).setBufferLength(lengths.get(0)));
    }
    return out.build();
  }
}
