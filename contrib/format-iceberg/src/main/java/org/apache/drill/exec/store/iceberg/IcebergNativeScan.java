/* Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0 . */
package org.apache.drill.exec.store.iceberg;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.nativeexecution.scan.NativeScanProvider;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.store.iceberg.read.IcebergColumnConverterFactory;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.util.PartitionUtil;

/**
 * Serializes planned file ranges, never replans the snapshot in the native reader.
 */
final class IcebergNativeScan {

  private IcebergNativeScan() {
  }

  static NativeScanProvider.Descriptor describe(IcebergSubScan scan) {
    if (scan.getMaxRecords() > 0) {
      return null;
    }
    try {
      Schema table = scan.getTableScan().table().schema();
      List<Types.NestedField> projected = new ArrayList<>();
      if (scan.getColumns().stream().anyMatch(SchemaPath::isDynamicStar)) {
        projected.addAll(scan.getTableScan().schema().columns());
      } else {
        Map<String, Types.NestedField> roots = new LinkedHashMap<>();
        for (SchemaPath path : scan.getColumns()) {
          Types.NestedField field = table.findField(path.getRootSegment().getPath());
          if (field == null) {
            return null;
          }
          // Read the original top-level field once. The native Project can
          // bind the requested struct members and array paths from that tree.
          roots.putIfAbsent(field.name(), field);
        }
        projected.addAll(roots.values());
      }
      // COUNT(*) may have an empty projection. The SDK reader must still read
      // a real column to obtain row counts, which is removed by the projection.
      Map<String, Types.NestedField> required = new LinkedHashMap<>();
      projected.forEach(field -> required.put(field.name(), field));
      if (required.isEmpty()) {
        required.put(table.columns().get(0).name(), table.columns().get(0));
      }
      if (!addFilterFields(scan.getCondition(), table, required)) {
        return null;
      }
      List<Types.NestedField> read = new ArrayList<>(required.values());
      List<MaterializedField> fields = fields(scan, read);
      List<MaterializedField> output = fields(scan, projected);
      if (fields == null || output == null) {
        return null;
      }
      ObjectMapper json = new ObjectMapper();
      Object schema = json.readTree(SchemaParser.toJson(new Schema(read)));
      List<Map<String, Object>> splits = new ArrayList<>();
      for (IcebergWork work : scan.getWorkList()) {
        for (FileScanTask task : work.getScanTask().files()) {
          // Reuse this task's planned ranges, partition constants and applicable
          // deletes. The worker must never replan a newer snapshot.
          if (task.file().format() != FileFormat.PARQUET || (task.residual() != Expressions.alwaysTrue() && scan.getCondition() == null)) {
            return null;
          }
          URI uri = URI.create(task.file().path().toString());
          if (uri.getScheme() != null && !uri.getScheme().equals("file")) {
            return null;
          }
          if (uri.getAuthority() != null && !uri.getAuthority().isEmpty()) {
            return null;
          }
          Map<String, Object> split = new LinkedHashMap<>();
          split.put("path", uri.getScheme() == null ? uri.toString() : uri.getPath());
          split.put("fileSize", task.file().fileSizeInBytes());
          split.put("start", task.start());
          split.put("length", task.length());
          split.put("schema", schema);
          split.put("batchSize", 8192);
          if (!task.spec().identitySourceIds().isEmpty()) {
            Map<Integer, ?> values = PartitionUtil.constantsMap(task);
            List<Map<String, Object>> constants = new ArrayList<>();
            for (int id : task.spec().identitySourceIds()) {
              Types.NestedField field = task.spec().schema().findField(id);
              Map<String, Object> constant = new LinkedHashMap<>();
              constant.put("fieldId", id);
              constant.put("type", json.readTree(SchemaParser.toJson(new Schema(field))).get("fields").get(0).get("type"));
              Object value = values.get(id);
              if (value == null) {
                constant.put("value", null);
              } else {
                ByteBuffer bytes = Conversions.toByteBuffer(field.type(), value).duplicate();
                List<Integer> encoded = new ArrayList<>(bytes.remaining());
                while (bytes.hasRemaining()) {
                  encoded.add(Byte.toUnsignedInt(bytes.get()));
                }
                constant.put("value", encoded);
              }
              constants.add(constant);
            }
            split.put("constants", constants);
          }
          if (!task.deletes().isEmpty()) {
            List<Map<String, Object>> deletes = new ArrayList<>();
            for (DeleteFile delete : task.deletes()) {
              if ((delete.format() != FileFormat.PARQUET && delete.format() != FileFormat.AVRO) || (delete.content() != FileContent.POSITION_DELETES && delete.content() != FileContent.EQUALITY_DELETES)) {
                return null;
              }
              URI location = URI.create(delete.path().toString());
              if ((location.getScheme() != null && !location.getScheme().equals("file")) || (location.getAuthority() != null && !location.getAuthority().isEmpty())) {
                return null;
              }
              Map<String, Object> descriptor = new LinkedHashMap<>();
              descriptor.put("path", location.getScheme() == null ? location.toString() : location.getPath());
              descriptor.put("format", delete.format().name());
              descriptor.put("content", delete.content().id());
              descriptor.put("fileSize", delete.fileSizeInBytes());
              descriptor.put("recordCount", delete.recordCount());
              descriptor.put("equalityIds", delete.equalityFieldIds() == null ? List.of() : delete.equalityFieldIds());
              deletes.add(descriptor);
            }
            split.put("deletes", deletes);
            // Position delete records contain this original URI, not the local
            // path used to open the data file through Arrow FileIO.
            split.put("dataFilePath", task.file().path().toString());
            split.put("tableSchema", json.readTree(SchemaParser.toJson(table)));
            List<Object> history = new ArrayList<>();
            for (Schema previous : scan.getTableScan().table().schemas().values()) {
              history.add(json.readTree(SchemaParser.toJson(previous)));
            }
            split.put("tableSchemas", history);
          }
          splits.add(split);
        }
      }
      return new NativeScanProvider.Descriptor("iceberg-parquet", 1, fields, output, scan.getCondition(), json.writeValueAsString(Map.of("format", "iceberg-parquet", "splits", splits, "snapshotId", scan.getTableScan().snapshot().snapshotId())));
    } catch (java.io.IOException | UnsupportedOperationException e) {
      return null;
    }
  }

  private static List<MaterializedField> fields(IcebergSubScan scan, List<Types.NestedField> fields) {
    List<MaterializedField> result = new ArrayList<>();
    for (Types.NestedField field : fields) {
      ColumnMetadata column = IcebergColumnConverterFactory.getColumnMetadata(field);
      if (scan.getSchema() != null && scan.getSchema().metadata(field.name()) != null) {
        ColumnMetadata provided = scan.getSchema().metadata(field.name());
        if (!sameColumn(column, provided)) {
          return null;
        }
        column = provided;
      }
      result.add(column.schema());
    }
    return result;
  }

  private static boolean sameColumn(ColumnMetadata column, ColumnMetadata provided) {
    if (provided.type() != column.type() || !sameMode(column.type(), column.mode(), provided.mode()) || provided.majorType().getPrecision() != column.majorType().getPrecision() || provided.majorType().getScale() != column.majorType().getScale() || !provided.properties().equals(column.properties())) {
      return false;
    }
    if (!sameFields(column.schema(), provided.schema())) {
      return false;
    }
    if (column.isMap() || column.isDict()) {
      var children = column.tupleSchema().iterator();
      var supplied = provided.tupleSchema().iterator();
      while (children.hasNext()) {
        if (!sameColumn(children.next(), supplied.next())) {
          return false;
        }
      }
    } else if (column.isMultiList()) {
      return sameColumn(column.childSchema(), provided.childSchema());
    }
    return true;
  }

  private static boolean sameMode(org.apache.drill.common.types.TypeProtos.MinorType minor, org.apache.drill.common.types.TypeProtos.DataMode actual, org.apache.drill.common.types.TypeProtos.DataMode provided) {
    // Original MapVector/DictVector have no parent validity in either mode.
    // Scalar/value/child modes are still checked recursively.
    return actual == provided || (minor == org.apache.drill.common.types.TypeProtos.MinorType.DICT || minor == org.apache.drill.common.types.TypeProtos.MinorType.MAP) && actual != org.apache.drill.common.types.TypeProtos.DataMode.REPEATED && provided != org.apache.drill.common.types.TypeProtos.DataMode.REPEATED;
  }

  private static boolean sameFields(MaterializedField column, MaterializedField provided) {
    var type = column.getType();
    var other = provided.getType();
    if (!column.getName().equals(provided.getName()) || type.getMinorType() != other.getMinorType() || !sameMode(type.getMinorType(), type.getMode(), other.getMode()) || type.getPrecision() != other.getPrecision() || type.getScale() != other.getScale() || column.getChildren().size() != provided.getChildren().size()) {
      return false;
    }
    var columns = column.getChildren().iterator();
    var supplied = provided.getChildren().iterator();
    while (columns.hasNext()) {
      if (!sameFields(columns.next(), supplied.next())) {
        return false;
      }
    }
    return true;
  }

  private static boolean addFilterFields(LogicalExpression expression, Schema table, Map<String, Types.NestedField> fields) {
    if (expression == null) {
      return true;
    }
    if (expression instanceof SchemaPath) {
      SchemaPath path = (SchemaPath) expression;
      Types.NestedField field = table.findField(path.getRootSegment().getPath());
      if (field == null) {
        return false;
      }
      fields.putIfAbsent(field.name(), field);
    }
    for (LogicalExpression child : expression) {
      if (!addFilterFields(child, table, fields)) {
        return false;
      }
    }
    return true;
  }
}
