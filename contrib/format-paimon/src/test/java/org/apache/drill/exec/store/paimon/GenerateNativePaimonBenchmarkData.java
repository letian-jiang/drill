/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.paimon;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.Assume;
import org.junit.Test;

/**
 * Opt-in data preparation; compares every value after reading the committed Paimon snapshot.
 */
public class GenerateNativePaimonBenchmarkData {

  private static final ObjectMapper JSON = new ObjectMapper();

  private static String sha256(byte[] bytes) throws Exception {
    return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
  }

  private static DataType type(String source) {
    if (source.startsWith("decimal(")) {
      String[] parameters = source.substring(8, source.length() - 1).split(",");
      return DataTypes.DECIMAL(Integer.parseInt(parameters[0].trim()), Integer.parseInt(parameters[1].trim()));
    }
    switch(source) {
      case "int":
        return DataTypes.INT();
      case "long":
        return DataTypes.BIGINT();
      case "date":
        return DataTypes.DATE();
      case "string":
        return DataTypes.STRING();
      default:
        throw new IllegalArgumentException("Unsupported benchmark source type " + source);
    }
  }

  private static Object value(Group row, int column, String source) {
    if (row.getFieldRepetitionCount(column) == 0) {
      return null;
    }
    if (source.startsWith("decimal(")) {
      org.apache.paimon.types.DecimalType decimal = (org.apache.paimon.types.DecimalType) type(source);
      PrimitiveTypeName physical = row.getType().getType(column).asPrimitiveType().getPrimitiveTypeName();
      BigInteger unscaled = physical == PrimitiveTypeName.INT64 ? BigInteger.valueOf(row.getLong(column, 0)) : physical == PrimitiveTypeName.INT32 ? BigInteger.valueOf(row.getInteger(column, 0)) : new BigInteger(row.getBinary(column, 0).getBytes());
      return Decimal.fromBigDecimal(new BigDecimal(unscaled, decimal.getScale()), decimal.getPrecision(), decimal.getScale());
    }
    switch(source) {
      case "int":
      case "date":
        return row.getInteger(column, 0);
      case "long":
        return row.getLong(column, 0);
      case "string":
        return BinaryString.fromBytes(row.getBinary(column, 0).getBytes());
      default:
        throw new IllegalArgumentException(source);
    }
  }

  // Four independent 64-bit modular sums of canonical SHA-256 row digests.
  // Row order may change when Paimon plans splits; count plus all four sums must match.
  private static final class ContentDigest {

    private final MessageDigest hash = MessageDigest.getInstance("SHA-256");

    private final long[] sums = new long[4];

    private long rows;

    private ContentDigest() throws Exception {
    }

    private void add(InternalRow row, InternalRow.FieldGetter[] getters) {
      hash.reset();
      for (InternalRow.FieldGetter getter : getters) {
        Object value = getter.getFieldOrNull(row);
        hash.update((byte) (value == null ? 0 : 1));
        if (value == null) {
          continue;
        }
        byte[] bytes = value instanceof BinaryString ? ((BinaryString) value).toBytes() : value instanceof Decimal ? ((Decimal) value).toBigDecimal().unscaledValue().toByteArray() : value.toString().getBytes(StandardCharsets.UTF_8);
        hash.update(ByteBuffer.allocate(4).putInt(bytes.length).array());
        hash.update(bytes);
      }
      ByteBuffer digest = ByteBuffer.wrap(hash.digest());
      for (int i = 0; i < sums.length; i++) {
        sums[i] += digest.getLong();
      }
      rows++;
    }

    private String result() {
      ByteBuffer buffer = ByteBuffer.allocate(32);
      for (long sum : sums) {
        buffer.putLong(sum);
      }
      return HexFormat.of().formatHex(buffer.array());
    }
  }

  @Test
  public void generate() throws Exception {
    String sourceArgument = System.getProperty("drill.native.source_dataset");
    String destinationArgument = System.getProperty("drill.native.paimon_dataset");
    Assume.assumeTrue("Set source_dataset and paimon_dataset explicitly", sourceArgument != null && destinationArgument != null);
    Path source = Path.of(sourceArgument).toAbsolutePath();
    Path destination = Path.of(destinationArgument).toAbsolutePath();
    if (Files.exists(destination)) {
      throw new IllegalStateException("Use a new dataset directory: " + destination);
    }
    Files.createDirectories(destination);
    byte[] sourceManifest = Files.readAllBytes(source.resolve("manifest.json"));
    JsonNode sourceTables = JSON.readTree(sourceManifest).get("tables");
    Map<String, Object> tables = new LinkedHashMap<>();
    Options options = new Options();
    options.set("warehouse", destination.resolve("warehouse").toUri().toString());
    options.set("metastore", "filesystem");
    Configuration hadoop = new Configuration();
    try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options, hadoop))) {
      catalog.createDatabase("default", false);
      for (var entry = sourceTables.fields(); entry.hasNext(); ) {
        var tableEntry = entry.next();
        String name = tableEntry.getKey();
        Path tablePath = source.resolve("warehouse").resolve(name);
        byte[] metadata = Files.readAllBytes(tablePath.resolve("metadata/v1.metadata.json"));
        if (!sha256(metadata).equals(tableEntry.getValue().get("metadata_sha256").asText())) {
          throw new IllegalStateException("Source metadata changed: " + name);
        }
        JsonNode fields = JSON.readTree(metadata).get("schemas").get(0).get("fields");
        Schema.Builder schema = Schema.newBuilder();
        InternalRow.FieldGetter[] getters = new InternalRow.FieldGetter[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
          DataType fieldType = type(fields.get(i).get("type").asText());
          schema.column(fields.get(i).get("name").asText(), fieldType);
          getters[i] = InternalRow.createFieldGetter(fieldType, i);
        }
        schema.option(CoreOptions.FILE_FORMAT.key(), "parquet").option(CoreOptions.TARGET_FILE_SIZE.key(), "32 mb").option(CoreOptions.SOURCE_SPLIT_TARGET_SIZE.key(), "16 mb");
        Identifier identifier = Identifier.create("default", name);
        catalog.createTable(identifier, schema.build(), false);
        Table table = catalog.getTable(identifier);
        var writeBuilder = table.newBatchWriteBuilder();
        List<CommitMessage> messages;
        ContentDigest expected = new ContentDigest();
        Map<String, String> inputFiles = new LinkedHashMap<>();
        List<Path> files;
        try (var paths = Files.list(tablePath.resolve("data"))) {
          files = paths.filter(p -> p.toString().endsWith(".parquet")).sorted().toList();
        }
        if (files.size() != tableEntry.getValue().get("data_files").asInt()) {
          throw new IllegalStateException("Source file count changed: " + name);
        }
        try (BatchTableWrite write = writeBuilder.newWrite()) {
          for (Path file : files) {
            inputFiles.put(source.relativize(file).toString(), sha256(Files.readAllBytes(file)));
            try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new org.apache.hadoop.fs.Path(file.toUri())).withConf(hadoop).build()) {
              Group group;
              while ((group = reader.read()) != null) {
                GenericRow row = new GenericRow(fields.size());
                for (int i = 0; i < fields.size(); i++) {
                  row.setField(i, value(group, i, fields.get(i).get("type").asText()));
                }
                expected.add(row, getters);
                write.write(row);
              }
            }
          }
          messages = write.prepareCommit();
        }
        try (BatchTableCommit commit = writeBuilder.newCommit()) {
          commit.commit(messages);
        }
        var readBuilder = table.newReadBuilder();
        var plan = readBuilder.newScan().plan();
        ContentDigest actual = new ContentDigest();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan.splits())) {
          RecordReader.RecordIterator<InternalRow> batch;
          while ((batch = reader.readBatch()) != null) {
            try {
              InternalRow row;
              while ((row = batch.next()) != null) {
                actual.add(row, getters);
              }
            } finally {
              batch.releaseBatch();
            }
          }
        }
        if (expected.rows != tableEntry.getValue().get("rows").asLong() || actual.rows != expected.rows || !actual.result().equals(expected.result())) {
          throw new IllegalStateException("Paimon round trip failed: " + name);
        }
        Map<String, String> outputFiles = new LinkedHashMap<>();
        Path committedTable = destination.resolve("warehouse/default.db").resolve(name);
        try (var paths = Files.walk(committedTable)) {
          for (Path file : paths.filter(Files::isRegularFile).sorted().toList()) {
            if (!file.getFileName().toString().endsWith(".crc")) {
              outputFiles.put(destination.relativize(file).toString(), sha256(Files.readAllBytes(file)));
            }
          }
        }
        tables.put(name, Map.of("rows", actual.rows, "content_sha256_mod64_sum", actual.result(), "planned_splits", plan.splits().size(), "source_files_sha256", inputFiles, "files_sha256", outputFiles));
        System.out.println("PAIMON_DATA " + name + " rows=" + actual.rows + " splits=" + plan.splits().size());
      }
    }
    Map<String, Object> manifest = new LinkedHashMap<>();
    manifest.put("scale_factor", JSON.readTree(sourceManifest).get("scale_factor").asInt());
    manifest.put("source_dataset", source.toString());
    manifest.put("format", "paimon");
    manifest.put("paimon_version", "1.3.1");
    manifest.put("source_dataset_manifest_sha256", sha256(sourceManifest));
    manifest.put("content_digest", "canonical per-row SHA-256; four 64-bit modular sums; null and field lengths included");
    manifest.put("tables", tables);
    Files.writeString(destination.resolve("manifest.json"), JSON.writerWithDefaultPrettyPrinter().writeValueAsString(manifest) + "\n");
  }
}
