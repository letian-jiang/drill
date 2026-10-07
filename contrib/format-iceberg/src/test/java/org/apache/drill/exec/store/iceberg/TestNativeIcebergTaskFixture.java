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
package org.apache.drill.exec.store.iceberg;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.junit.Assume;
import org.junit.Test;

/**
 * Real HadoopTables metadata and applicable v2 deletes; never uses engine output as an oracle.
 */
public class TestNativeIcebergTaskFixture {

  private Path output;

  private final List<Map<String, Object>> receipts = new ArrayList<>();

  private static final int ROWS = 130003;

  private static final Map<String, String> PROPERTIES = Map.of("format-version", "2", "write.parquet.row-group-size-bytes", "65536", "write.parquet.page-size-bytes", "8192", "read.split.target-size", "65536");

  private Table table(String name, Schema schema, PartitionSpec spec) {
    return new HadoopTables(new Configuration()).create(schema, spec, SortOrder.unsorted(), PROPERTIES, output.resolve("warehouse").resolve(name).toUri().toString());
  }

  private DataFile data(Table table, Schema physical, GenericRecord partition, int number, int first, int count, java.util.function.IntFunction<GenericRecord> record) throws Exception {
    String path = Path.of(java.net.URI.create(table.location())).resolve("data").resolve("part" + number + ".parquet").toUri().toString();
    var factory = new GenericAppenderFactory(physical, table.spec()).setAll(PROPERTIES);
    var writer = factory.newDataWriter(EncryptedFiles.encryptedOutput(table.io().newOutputFile(path), (ByteBuffer) null), FileFormat.PARQUET, partition);
    for (int i = 0; i < count; i++) {
      writer.add(record.apply(first + i));
    }
    writer.close();
    var file = writer.toDataFile();
    receipts.add(Map.of("table", table.name(), "path", path, "first", first, "rows", count, "splitOffsets", file.splitOffsets()));
    return file;
  }

  private void identity() throws Exception {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()), Types.NestedField.optional(2, "p", Types.StringType.get()), Types.NestedField.optional(3, "k", Types.LongType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("p").build();
    Table table = table("identity_tasks", schema, spec);
    Schema physical = new Schema(schema.findField("id"), schema.findField("k"));
    Schema equality = new Schema(schema.findField("k"));
    List<DataFile> files = new ArrayList<>();
    List<DeleteFile> deletes = new ArrayList<>();
    for (int part = 0, first = 0; first < ROWS; part++, first += 32501) {
      int count = Math.min(32501, ROWS - first);
      var partition = GenericRecord.create(spec.partitionType());
      partition.set(0, part % 2 == 0 ? "雪" : null);
      DataFile file = data(table, physical, partition, part, first, count, id -> {
        var row = GenericRecord.create(physical);
        row.setField("id", (long) id);
        row.setField("k", id % 29 == 0 ? null : (long) (id % 37));
        return row;
      });
      files.add(file);
      var factory = new GenericAppenderFactory(schema, spec, new int[] { 3 }, equality, null);
      FileFormat format = part % 2 == 0 ? FileFormat.PARQUET : FileFormat.AVRO;
      String prefix = Path.of(java.net.URI.create(table.location())).resolve("data").resolve("deletes" + part).toUri().toString();
      var position = factory.newPosDeleteWriter(EncryptedFiles.encryptedOutput(table.io().newOutputFile(prefix + "-pos." + format.name().toLowerCase()), (ByteBuffer) null), format, partition);
      for (long pos : new long[] { 0, 8191, 8192, count - 1 }) {
        position.delete(file.path(), pos);
      }
      position.close();
      deletes.add(position.toDeleteFile());
      var eq = factory.newEqDeleteWriter(EncryptedFiles.encryptedOutput(table.io().newOutputFile(prefix + "-eq." + format.name().toLowerCase()), (ByteBuffer) null), format, partition);
      for (Long key : new Long[] { null, 5L, 17L }) {
        var row = GenericRecord.create(equality);
        row.setField("k", key);
        eq.delete(row);
      }
      eq.close();
      deletes.add(eq.toDeleteFile());
    }
    var append = table.newAppend();
    files.forEach(append::appendFile);
    append.commit();
    var delta = table.newRowDelta();
    deletes.forEach(delta::addDeletes);
    delta.commit();
  }

  private void transformed() throws Exception {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()), Types.NestedField.optional(2, "k", Types.LongType.get()), Types.NestedField.optional(3, "d", Types.DateType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("k", 8).day("d").build();
    Table table = table("transform_tasks", schema, spec);
    var append = table.newAppend();
    for (int part = 0; part < 3; part++) {
      int number = part;
      var partition = GenericRecord.create(spec.partitionType());
      long key = part == 0 ? 3 : part == 1 ? 7 : 31;
      LocalDate day = LocalDate.of(2020, 1, 1).plusDays(part);
      partition.set(0, Transforms.bucket(Types.LongType.get(), 8).apply(key));
      partition.set(1, (int) day.toEpochDay());
      append.appendFile(data(table, schema, partition, part, part * 17003, 17003, id -> {
        var row = GenericRecord.create(schema);
        row.setField("id", (long) id);
        row.setField("k", key);
        row.setField("d", LocalDate.of(2020, 1, 1).plusDays(number));
        return row;
      }));
    }
    append.commit();
  }

  private void nested() throws Exception {
    Types.StructType members = Types.StructType.of(Types.NestedField.optional(8, "x", Types.LongType.get()), Types.NestedField.required(9, "p", Types.StringType.get()));
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()), Types.NestedField.required(7, "m", members));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("m.p").build();
    Table table = table("nested_tasks", schema, spec);
    // Table creation assigns fresh field IDs in Iceberg 0.12. Write the
    // committed IDs, including nested members, into the Parquet schema.
    schema = table.schema();
    members = schema.findField("m").type().asStructType();
    Types.StructType dataMembers = Types.StructType.of(members.field("x"));
    Schema physical = new Schema(schema.findField("id"), Types.NestedField.required(schema.findField("m").fieldId(), "m", dataMembers));
    var partition = GenericRecord.create(table.spec().partitionType());
    partition.set(0, "part雪");
    DataFile file = data(table, physical, partition, 0, 0, ROWS, id -> {
      var row = GenericRecord.create(physical);
      var m = GenericRecord.create(dataMembers);
      m.setField("x", (long) (id % 47));
      row.setField("id", (long) id);
      row.setField("m", m);
      return row;
    });
    table.newAppend().appendFile(file).commit();
  }

  private void onlyConstant() throws Exception {
    Schema schema = new Schema(Types.NestedField.required(2, "p", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("p").build();
    Table table = table("constant_tasks", schema, spec);
    Schema physical = new Schema(Types.NestedField.required(100, "anchor", Types.LongType.get()));
    var partition = GenericRecord.create(spec.partitionType());
    partition.set(0, "constant雪");
    DataFile file = data(table, physical, partition, 0, 0, ROWS, id -> {
      var row = GenericRecord.create(physical);
      row.setField("anchor", (long) id);
      return row;
    });
    table.newAppend().appendFile(file).commit();
  }

  @Test
  public void generateImmutableTables() throws Exception {
    String directory = System.getProperty("drill.native.task_fixture");
    Assume.assumeNotNull(directory);
    output = Path.of(directory);
    if (Files.exists(output)) {
      throw new IllegalStateException("Fixture must use a new directory");
    }
    Files.createDirectories(output);
    identity();
    transformed();
    nested();
    onlyConstant();
    new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(output.resolve("java-files.json").toFile(), receipts);
  }
}
