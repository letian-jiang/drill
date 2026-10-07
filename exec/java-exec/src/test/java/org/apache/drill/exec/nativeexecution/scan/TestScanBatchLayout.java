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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import java.util.ArrayList;
import java.util.List;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.junit.Test;

public class TestScanBatchLayout {

  private static MaterializedField field(String name, MinorType minor, DataMode mode) {
    return MaterializedField.create(name, MajorType.newBuilder().setMinorType(minor).setMode(mode).setPrecision(18).setScale(2).build());
  }

  private static BatchSchema schema(List<MaterializedField> fields) {
    return new BatchSchema(BatchSchema.SelectionVectorMode.NONE, fields);
  }

  @Test
  public void fixedLayoutPreservesNullableAndVariableBufferShapes() {
    var fields = List.of(field("i", MinorType.INT, DataMode.REQUIRED), field("l", MinorType.BIGINT, DataMode.OPTIONAL), field("s", MinorType.VARCHAR, DataMode.OPTIONAL), field("d", MinorType.DATE, DataMode.REQUIRED), field("b", MinorType.BIT, DataMode.OPTIONAL), field("n", MinorType.VARDECIMAL, DataMode.OPTIONAL), field("ts", MinorType.TIMESTAMP, DataMode.OPTIONAL), field("bin", MinorType.VARBINARY, DataMode.OPTIONAL), field("time", MinorType.TIME, DataMode.OPTIONAL));
    var layout = new ScanBatchLayout(schema(fields));
    assertArrayEquals(new int[] { 1, 2, 3, 1, 2, 3, 2, 3, 2 }, layout.bufferCounts);
    assertEquals(19, layout.addresses.length);
    assertEquals(19, layout.lengths.length);
    layout.validate(schema(new ArrayList<>(fields)));
  }

  @Test
  public void schemaSnapshotDetectsMutationOfTheSameSchemaObject() {
    List<MaterializedField> fields = new ArrayList<>();
    fields.add(field("v", MinorType.BIGINT, DataMode.OPTIONAL));
    var batch = schema(fields);
    var layout = new ScanBatchLayout(batch);
    for (var replacement : List.of(field("v", MinorType.INT, DataMode.OPTIONAL), field("renamed", MinorType.BIGINT, DataMode.OPTIONAL), field("v", MinorType.BIGINT, DataMode.REQUIRED), MaterializedField.create("v", fields.get(0).getType().toBuilder().setScale(3).build()))) {
      fields.set(0, replacement);
      try {
        layout.validate(batch);
        fail("Schema change was accepted");
      } catch (IllegalStateException expected) {
      }
    }
  }

  @Test
  public void layoutRejectsSelectionVectorsAndTracksRepeatedColumns() {
    var fields = List.of(field("v", MinorType.INT, DataMode.REQUIRED));
    try {
      new ScanBatchLayout(new BatchSchema(BatchSchema.SelectionVectorMode.TWO_BYTE, fields));
      fail("Selection vector was accepted");
    } catch (IllegalStateException expected) {
    }
    var repeated = new ScanBatchLayout(schema(List.of(field("v", MinorType.INT, DataMode.REPEATED))));
    assertArrayEquals(new int[] { 2 }, repeated.bufferCounts);
  }

  @Test
  public void mapLayoutTracksNestedShapesAndChildMutations() {
    var map = field("m", MinorType.MAP, DataMode.REQUIRED);
    var inner = field("inner", MinorType.MAP, DataMode.REQUIRED);
    inner.addChild(field("s", MinorType.VARCHAR, DataMode.OPTIONAL));
    map.addChild(field("n", MinorType.INT, DataMode.REQUIRED));
    map.addChild(inner);
    var layout = new ScanBatchLayout(schema(List.of(map)));
    assertArrayEquals(new int[] { 4 }, layout.bufferCounts);
    assertEquals(4, layout.addresses.length);
    layout.validate(schema(List.of(map.clone())));
    inner.addChild(field("later", MinorType.BIGINT, DataMode.OPTIONAL));
    try {
      layout.validate(schema(List.of(map)));
      fail("Nested schema mutation was accepted");
    } catch (IllegalStateException expected) {
    }
    var optional = field("m", MinorType.MAP, DataMode.OPTIONAL);
    optional.addChild(field("n", MinorType.INT, DataMode.REQUIRED));
    optional.addChild(inner.clone());
    var optionalLayout = new ScanBatchLayout(schema(List.of(optional)));
    assertArrayEquals(new int[] { 6 }, optionalLayout.bufferCounts);
    optionalLayout.validate(schema(List.of(optional.clone())));
    try {
      layout.validate(schema(List.of(optional)));
      fail("MAP mode/schema mutation was accepted");
    } catch (IllegalStateException expected) {
    }
    var empty = new ScanBatchLayout(schema(List.of(field("empty", MinorType.MAP, DataMode.REQUIRED))));
    assertArrayEquals(new int[] { 0 }, empty.bufferCounts);
    assertEquals(0, empty.addresses.length);
  }

  @Test
  public void arrayLayoutTracksElementTypesAndNestedLists() {
    var map = field("maps", MinorType.MAP, DataMode.REPEATED);
    map.addChild(field("n", MinorType.INT, DataMode.OPTIONAL));
    var list = field("list", MinorType.LIST, DataMode.OPTIONAL);
    list.addChild(field("$data$", MinorType.VARCHAR, DataMode.OPTIONAL));
    var nested = field("nested", MinorType.LIST, DataMode.REPEATED);
    nested.addChild(field("$data$", MinorType.INT, DataMode.REPEATED));
    var layout = new ScanBatchLayout(schema(List.of(map, list, nested)));
    assertArrayEquals(new int[] { 3, 5, 3 }, layout.bufferCounts);
    list.removeChild(list.getChildren().iterator().next());
    list.addChild(field("$data$", MinorType.INT, DataMode.OPTIONAL));
    try {
      layout.validate(schema(List.of(map, list, nested)));
      fail("Array element mutation accepted");
    } catch (IllegalStateException expected) {
    }
  }
}
