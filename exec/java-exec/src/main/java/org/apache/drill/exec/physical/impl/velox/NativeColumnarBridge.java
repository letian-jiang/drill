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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;

/**
 * Fixed schema descriptors and buffer layout for minor plans and JNI scan.
 */
public final class NativeColumnarBridge {

  private NativeColumnarBridge() {
  }

  static void validateType(MajorType type) {
    if (type.getMinorType() == MinorType.LIST || type.getMinorType() == MinorType.DICT) {
      return;
    }
    if (type.getMinorType() == MinorType.MAP) {
      // Both modes use original MapVector: child buffers, no parent validity.
      return;
    }
    if (!java.util.Set.of(MinorType.INT, MinorType.BIGINT, MinorType.FLOAT4, MinorType.FLOAT8, MinorType.BIT, MinorType.VARCHAR, MinorType.VARBINARY, MinorType.DATE, MinorType.TIME, MinorType.TIMESTAMP, MinorType.VARDECIMAL).contains(type.getMinorType())) {
      throw new UnsupportedOperationException("Unsupported native columnar type " + type);
    }
  }

  public static boolean isArray(MaterializedField field) {
    return field.getType().getMode() == DataMode.REPEATED || field.getType().getMinorType() == MinorType.LIST;
  }

  private static MaterializedField arrayElement(MaterializedField field) {
    if (field.getType().getMinorType() != MinorType.LIST) {
      var element = MaterializedField.create("$data$", field.getType().toBuilder().setMode(DataMode.REQUIRED).build());
      if (field.getType().getMinorType() == MinorType.MAP) {
        for (var child : field.getChildren()) {
          element.addChild(child.clone());
        }
      }
      if (field.getType().getMinorType() == MinorType.DICT) {
        var children = field.getChildren();
        // Original RepeatedDictVector exposes an $inner$ DictVector child.
        if (children.size() == 1 && children.iterator().next().getType().getMinorType() == MinorType.DICT) {
          children = children.iterator().next().getChildren();
        }
        for (var child : children) {
          element.addChild(child.clone());
        }
      }
      return element;
    }
    // LIST fields may still contain the historical dummy LATE child. The
    // homogeneous data child is the only supported element, never UNION.
    var candidates = field.getChildren().stream().filter(child -> child.getType().getMinorType() != MinorType.LATE && !child.getName().equals("$offsets$") && !child.getName().equals("$bits$")).toList();
    if (candidates.size() != 1) {
      throw new UnsupportedOperationException("Native LIST requires one fixed element type " + field);
    }
    return candidates.get(0).clone();
  }

  public static List<Map<String, Object>> fields(Iterable<MaterializedField> schema) {
    List<Map<String, Object>> fields = new ArrayList<>();
    for (MaterializedField field : schema) {
      validateType(field.getType());
      Map<String, Object> descriptor = new LinkedHashMap<>(Map.of("name", field.getName(), "minor", field.getType().getMinorType().name(), "optional", field.getType().getMode() == DataMode.OPTIONAL, "precision", field.getType().getPrecision(), "scale", field.getType().getScale(), "type", java.util.Base64.getEncoder().encodeToString(field.getType().toByteArray())));
      if (isArray(field)) {
        descriptor.put("repeated", field.getType().getMode() == DataMode.REPEATED);
        descriptor.put("element", fields(List.of(arrayElement(field))).get(0));
      } else if (field.getType().getMinorType() == MinorType.MAP || field.getType().getMinorType() == MinorType.DICT) {
        descriptor.put("children", fields(field.getChildren()));
      }
      fields.add(descriptor);
    }
    return fields;
  }

  public static int bufferCount(MaterializedField field) {
    validateType(field.getType());
    if (isArray(field)) {
      return (field.getType().getMode() == DataMode.REPEATED ? 1 : 2) + bufferCount(arrayElement(field));
    }
    if (field.getType().getMinorType() == MinorType.MAP || field.getType().getMinorType() == MinorType.DICT) {
      int count = field.getType().getMinorType() == MinorType.DICT ? 1 : 0;
      for (MaterializedField child : field.getChildren()) {
        count += bufferCount(child);
      }
      return count;
    }
    MinorType minor = field.getType().getMinorType();
    int values = minor == MinorType.VARCHAR || minor == MinorType.VARBINARY || minor == MinorType.VARDECIMAL ? 2 : 1;
    return values + (field.getType().getMode() == DataMode.OPTIONAL ? 1 : 0);
  }

}
