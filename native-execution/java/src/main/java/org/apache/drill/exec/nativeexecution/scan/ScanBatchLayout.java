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
package org.apache.drill.exec.nativeexecution.scan;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.physical.impl.velox.NativeColumnarBridge;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;

/**
 * Fixed column tree and flattened buffers, independent of mutable plugin schema objects.
 */
final class ScanBatchLayout {

  private final FieldSnapshot[] fields;

  final int[] bufferCounts;

  final long[] addresses;

  final long[] lengths;

  ScanBatchLayout(BatchSchema schema) {
    int columns = schema.getFieldCount();
    fields = new FieldSnapshot[columns];
    bufferCounts = new int[columns];
    int buffers = 0;
    for (int i = 0; i < columns; i++) {
      var field = schema.getColumn(i);
      fields[i] = new FieldSnapshot(field);
      bufferCounts[i] = NativeColumnarBridge.bufferCount(field);
      buffers += bufferCounts[i];
    }
    addresses = new long[buffers];
    lengths = new long[buffers];
    validate(schema);
  }

  void validate(BatchSchema schema) {
    if (schema.getSelectionVectorMode() != BatchSchema.SelectionVectorMode.NONE || schema.getFieldCount() != fields.length) {
      throw new IllegalStateException("JNI plugin schema changed during a native Task");
    }
    for (int i = 0; i < fields.length; i++) {
      fields[i].validate(schema.getColumn(i));
    }
  }

  private static final class FieldSnapshot {

    final String name;

    final MajorType type;

    final FieldSnapshot[] children;

    FieldSnapshot(MaterializedField field) {
      name = field.getName();
      // protobuf MajorType is immutable
      type = field.getType();
      children = field.getChildren().stream().map(FieldSnapshot::new).toArray(FieldSnapshot[]::new);
    }

    void validate(MaterializedField field) {
      if (!name.equals(field.getName()) || !type.equals(field.getType()) || children.length != field.getChildren().size()) {
        throw new IllegalStateException("JNI plugin schema changed during a native Task");
      }
      int index = 0;
      for (MaterializedField child : field.getChildren()) {
        children[index++].validate(child);
      }
    }
  }
}
