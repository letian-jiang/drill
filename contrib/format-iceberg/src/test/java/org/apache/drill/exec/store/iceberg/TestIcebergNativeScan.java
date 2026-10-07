/* Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0 . */
package org.apache.drill.exec.store.iceberg;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.parser.LogicalExpressionParser;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.nullable;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestIcebergNativeScan {

  private final Schema schema = new Schema(Types.NestedField.optional(10, "n", Types.IntegerType.get()), Types.NestedField.optional(20, "label", Types.StringType.get()));

  private final DataFile file = mock(DataFile.class);

  private final FileScanTask task = mock(FileScanTask.class);

  @Test
  public void javaScanProjectsWholeMapForDictKeyPaths() {
    Schema complex = new Schema(Types.NestedField.optional(1, "d", Types.MapType.ofOptional(2, 3, Types.StringType.get(), Types.LongType.get())), Types.NestedField.required(4, "s", Types.StructType.of(Types.NestedField.optional(5, "n", Types.LongType.get()), Types.NestedField.optional(6, "m", Types.MapType.ofOptional(7, 8, Types.StringType.get(), Types.LongType.get())))));
    TableScan original = mock(TableScan.class);
    when(original.schema()).thenReturn(complex);
    when(original.select(anyList())).thenReturn(original);
    IcebergGroupScan.projectColumns(original, List.of(SchemaPath.getCompoundPath("d", "a"), SchemaPath.getCompoundPath("d", "b"), SchemaPath.getCompoundPath("s", "n"), SchemaPath.getCompoundPath("s", "m", "key")));
    verify(original).select(List.of("d", "s.n", "s.m"));
  }

  @Test
  public void providedRequiredDictKeepsSdkWithoutChangingNullableValueSemantics() {
    var builder = scan();
    Schema dict = new Schema(Types.NestedField.optional(1, "d", Types.MapType.ofOptional(2, 3, Types.StringType.get(), Types.LongType.get())));
    when(builder.build().getTableScan().table().schema()).thenReturn(dict);
    var columns = List.of(SchemaPath.getCompoundPath("d", "key"));
    var required = new SchemaBuilder().addDict("d", MinorType.VARCHAR).value(MinorType.BIGINT).resumeSchema().build();
    assertNotNull(builder.columns(columns).schema(required).build().nativeScan());
    var nullable = new SchemaBuilder().addDict("d", MinorType.VARCHAR).nullableValue(MinorType.BIGINT).resumeSchema().build();
    assertNull(builder.columns(columns).schema(nullable).build().nativeScan());
  }

  @Test
  public void nullableStructPreservesModeAndRequiredProvidedMapKeepsSdk() {
    var builder = scan();
    Schema struct = new Schema(Types.NestedField.optional(1, "m", Types.StructType.of(Types.NestedField.optional(2, "n", Types.LongType.get()), Types.NestedField.optional(3, "inner", Types.StructType.of(Types.NestedField.optional(4, "text", Types.StringType.get()))))));
    when(builder.build().getTableScan().table().schema()).thenReturn(struct);
    builder.columns(List.of(SchemaPath.getSimplePath("m")));
    var original = builder.build().nativeScan();
    assertNotNull(original);
    assertEquals(org.apache.drill.common.types.TypeProtos.DataMode.OPTIONAL, original.outputFields.get(0).getType().getMode());
    var required = new SchemaBuilder().addMap("m").addNullable("n", MinorType.BIGINT).addMap("inner").addNullable("text", MinorType.VARCHAR).resumeMap().resumeSchema().build();
    assertNotNull(builder.schema(required).build().nativeScan());
    var changedChild = new SchemaBuilder().addMap("m").add("n", MinorType.BIGINT).addMap("inner").addNullable("text", MinorType.VARCHAR).resumeMap().resumeSchema().build();
    assertNull(builder.schema(changedChild).build().nativeScan());
  }

  private IcebergSubScan.IcebergSubScanBuilder scan() {
    when(file.format()).thenReturn(FileFormat.PARQUET);
    when(file.path()).thenReturn("file:///tmp/native%20scan.parquet");
    when(file.fileSizeInBytes()).thenReturn(1000L);
    when(task.file()).thenReturn(file);
    when(task.start()).thenReturn(4L);
    when(task.length()).thenReturn(996L);
    when(task.spec()).thenReturn(PartitionSpec.unpartitioned());
    when(task.deletes()).thenReturn(List.of());
    when(task.residual()).thenReturn(Expressions.alwaysTrue());
    CombinedScanTask combined = mock(CombinedScanTask.class);
    when(combined.files()).thenReturn(List.of(task));
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(schema);
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.snapshotId()).thenReturn(47L);
    TableScan scan = mock(TableScan.class);
    when(scan.table()).thenReturn(table);
    when(scan.schema()).thenReturn(schema);
    when(scan.snapshot()).thenReturn(snapshot);
    return IcebergSubScan.builder().userName("test").tableScan(scan).maxRecords(0).columns(List.of(SchemaPath.getSimplePath("label"))).workList(List.of(new IcebergWork(combined)));
  }

  @Test
  public void preservesFieldIdsSnapshotAndFileRange() throws Exception {
    var descriptor = scan().build().nativeScan();
    assertEquals("iceberg-parquet", descriptor.provider);
    assertEquals(1, descriptor.version);
    assertNotNull(descriptor);
    var json = new ObjectMapper().readTree(descriptor.json);
    assertEquals(47L, json.get("snapshotId").asLong());
    var split = json.get("splits").get(0);
    assertEquals("/tmp/native scan.parquet", split.get("path").asText());
    assertEquals(4, split.get("start").asInt());
    assertEquals(996, split.get("length").asInt());
    assertEquals(20, split.get("schema").get("fields").get(0).get("id").asInt());
  }

  @Test
  public void retainsUnprojectedFilterColumnsForNativeFilter() {
    var filter = LogicalExpressionParser.parse("greater_than(`n`, 5)");
    var descriptor = scan().condition(filter).build().nativeScan();
    assertNotNull(descriptor);
    assertSame(filter, descriptor.filter);
    assertEquals(2, descriptor.readFields.size());
    assertEquals("n", descriptor.readFields.get(1).getName());
    assertEquals(1, descriptor.outputFields.size());
  }

  @Test
  public void preservesApplicablePositionAndEqualityDeletes() throws Exception {
    var scan = scan().build();
    DeleteFile position = mock(DeleteFile.class), equality = mock(DeleteFile.class);
    for (DeleteFile delete : List.of(position, equality)) {
      when(delete.format()).thenReturn(FileFormat.PARQUET);
      when(delete.path()).thenReturn("file:///tmp/delete%20file.parquet");
      when(delete.fileSizeInBytes()).thenReturn(77L);
      when(delete.recordCount()).thenReturn(3L);
    }
    when(position.content()).thenReturn(FileContent.POSITION_DELETES);
    when(equality.content()).thenReturn(FileContent.EQUALITY_DELETES);
    when(equality.equalityFieldIds()).thenReturn(List.of(10));
    when(task.deletes()).thenReturn(List.of(position, equality));
    var descriptor = scan.nativeScan();
    assertNotNull(descriptor);
    var split = new ObjectMapper().readTree(descriptor.json).get("splits").get(0);
    assertEquals("file:///tmp/native%20scan.parquet", split.get("dataFilePath").asText());
    assertEquals(1, split.get("deletes").get(0).get("content").asInt());
    assertEquals("/tmp/delete file.parquet", split.get("deletes").get(1).get("path").asText());
    assertEquals(10, split.get("deletes").get(1).get("equalityIds").get(0).asInt());
    assertEquals(10, split.get("tableSchema").get("fields").get(0).get("id").asInt());
    when(equality.path()).thenReturn("s3://bucket/delete.parquet");
    assertNull(scan.nativeScan());
    when(equality.path()).thenReturn("file:///tmp/delete.parquet");
    when(equality.format()).thenReturn(FileFormat.AVRO);
    assertNotNull(scan.nativeScan());
    when(equality.format()).thenReturn(FileFormat.ORC);
    assertNull(scan.nativeScan());
  }

  @Test
  public void preservesIdentityConstantsAndAcceptsTransformedPartitions() throws Exception {
    var scan = scan().build();
    when(task.spec()).thenReturn(PartitionSpec.builderFor(schema).identity("n").build());
    StructLike partition = mock(StructLike.class);
    when(file.partition()).thenReturn(partition);
    when(partition.get(0, Object.class)).thenReturn(-17);
    var json = new ObjectMapper().readTree(scan.nativeScan().json);
    var constant = json.get("splits").get(0).get("constants").get(0);
    assertEquals(10, constant.get("fieldId").asInt());
    assertEquals("int", constant.get("type").asText());
    assertEquals(239, constant.get("value").get(0).asInt());
    assertEquals(255, constant.get("value").get(3).asInt());
    when(partition.get(0, Object.class)).thenReturn(null);
    assertTrue(new ObjectMapper().readTree(scan.nativeScan().json).get("splits").get(0).get("constants").get(0).get("value").isNull());
    when(task.spec()).thenReturn(PartitionSpec.builderFor(schema).bucket("n", 8).build());
    assertNotNull(scan.nativeScan());
    when(file.path()).thenReturn("s3://bucket/data.parquet");
    assertNull(scan.nativeScan());
  }

  @Test
  public void rejectsResidualWithoutExecutableCondition() {
    var scan = scan().build();
    when(task.residual()).thenReturn(Expressions.greaterThan("n", 5));
    assertNull(scan.nativeScan());
  }

  @Test
  public void rejectsLimitsAndProvidedSchemaConversions() {
    assertNull(scan().maxRecords(10).build().nativeScan());
    assertNull(scan().schema(new SchemaBuilder().addNullable("label", MinorType.INT).build()).build().nativeScan());
  }

  @Test
  public void emptyProjectionStillReadsRowCounts() {
    var descriptor = scan().columns(List.of()).build().nativeScan();
    assertNotNull(descriptor);
    assertEquals(1, descriptor.readFields.size());
    assertEquals(0, descriptor.outputFields.size());
  }

  @Test
  public void timestampAndBinaryFieldsUseNativeSdkWithTheirOriginalFieldIds() throws Exception {
    var builder = scan();
    Schema typed = new Schema(Types.NestedField.optional(30, "ts", Types.TimestampType.withoutZone()), Types.NestedField.optional(40, "bytes", Types.BinaryType.get()), Types.NestedField.optional(50, "utc_ts", Types.TimestampType.withZone()));
    when(builder.build().getTableScan().table().schema()).thenReturn(typed);
    var descriptor = builder.columns(List.of(SchemaPath.getSimplePath("ts"), SchemaPath.getSimplePath("bytes"), SchemaPath.getSimplePath("utc_ts"))).build().nativeScan();
    assertNotNull(descriptor);
    var fields = new ObjectMapper().readTree(descriptor.json).get("splits").get(0).get("schema").get("fields");
    assertEquals("timestamp", fields.get(0).get("type").asText());
    assertEquals("binary", fields.get(1).get("type").asText());
    assertEquals("timestamptz", fields.get(2).get("type").asText());
    assertEquals(50, fields.get(2).get("id").asInt());
  }

  @Test
  public void nestedProjectionsKeepWholeRootOnceAndPreserveFieldIds() throws Exception {
    var builder = scan();
    Schema nested = new Schema(Types.NestedField.required(30, "m", Types.StructType.of(Types.NestedField.optional(31, "n", Types.LongType.get()), Types.NestedField.optional(32, "ts", Types.TimestampType.withoutZone()), Types.NestedField.optional(33, "values", Types.ListType.ofOptional(34, Types.StringType.get())))));
    when(builder.build().getTableScan().table().schema()).thenReturn(nested);
    var descriptor = builder.columns(List.of(SchemaPath.getCompoundPath("m", "n"), SchemaPath.getCompoundPath("m", "ts"))).build().nativeScan();
    assertNotNull(descriptor);
    assertEquals(1, descriptor.readFields.size());
    assertEquals(1, descriptor.outputFields.size());
    assertEquals(3, descriptor.outputFields.get(0).getChildren().size());
    var fields = new ObjectMapper().readTree(descriptor.json).get("splits").get(0).get("schema").get("fields").get(0).get("type").get("fields");
    assertEquals(31, fields.get(0).get("id").asInt());
    assertEquals(34, fields.get(2).get("type").get("element-id").asInt());
  }

  @Test
  public void retainsNestedFilterRootAndRejectsNestedProvidedConversion() {
    var builder = scan();
    Schema nested = new Schema(Types.NestedField.optional(20, "label", Types.StringType.get()), Types.NestedField.required(30, "m", Types.StructType.of(Types.NestedField.optional(31, "n", Types.LongType.get()))));
    when(builder.build().getTableScan().table().schema()).thenReturn(nested);
    var descriptor = builder.condition(LogicalExpressionParser.parse("greater_than(`m`.`n`, 5)")).build().nativeScan();
    assertNotNull(descriptor);
    assertEquals("m", descriptor.readFields.get(1).getName());
    assertEquals(1, descriptor.outputFields.size());
    var provided = new SchemaBuilder().addMap("m").addNullable("n", MinorType.VARCHAR).resumeSchema().build();
    assertNull(builder.columns(List.of(SchemaPath.getSimplePath("m"))).schema(provided).build().nativeScan());
  }
}
