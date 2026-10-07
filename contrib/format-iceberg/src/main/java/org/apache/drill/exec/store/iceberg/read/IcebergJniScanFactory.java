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
package org.apache.drill.exec.store.iceberg.read;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.nativeexecution.scan.PluginScanFactory;
import org.apache.drill.exec.nativeexecution.scan.ScanServices;
import org.apache.drill.exec.physical.impl.scan.ScanOperatorExec;
import org.apache.drill.exec.physical.impl.scan.framework.BasicScanFactory;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedScanFramework;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.iceberg.IcebergGroupScan;
import org.apache.drill.exec.store.iceberg.IcebergWork;
import org.apache.drill.exec.store.iceberg.format.IcebergFormatPluginConfig;
import org.apache.hadoop.conf.Configuration;

/**
 * Original Iceberg reader for JNI scans, with no Java fragment or Drillbit.
 */
public final class IcebergJniScanFactory implements PluginScanFactory {

  @Override
  public ScanOperatorExec open(JsonNode scan, ScanServices services) throws Exception {
    var mapper = services.mapper();
    Configuration fs = new Configuration();
    var properties = scan.path("fsConf").fields();
    while (properties.hasNext()) {
      var property = properties.next();
      fs.set(property.getKey(), property.getValue().asText());
    }
    var format = mapper.treeToValue(scan.get("format"), IcebergFormatPluginConfig.class);
    List<SchemaPath> columns = new ArrayList<>();
    for (var column : scan.get("columns")) {
      columns.add(mapper.treeToValue(column, SchemaPath.class));
    }
    var condition = scan.hasNonNull("condition") ? mapper.treeToValue(scan.get("condition"), LogicalExpression.class) : null;
    var tableScan = IcebergGroupScan.initTableScan(fs, format, scan.get("path").asText(), condition);
    if (scan.hasNonNull("plannedSnapshotId")) {
      tableScan = tableScan.useSnapshot(scan.get("plannedSnapshotId").asLong());
    }
    tableScan = IcebergGroupScan.projectColumns(tableScan, columns);
    List<ManagedReader<SchemaNegotiator>> readers = new ArrayList<>();
    for (var work : scan.get("workList")) {
      readers.add(new IcebergRecordReader(tableScan, mapper.treeToValue(work, IcebergWork.class), scan.get("maxRecords").asInt()));
    }
    var schema = scan.hasNonNull("schema") ? mapper.treeToValue(scan.get("schema"), TupleMetadata.class) : null;
    var builder = new ManagedScanFramework.ScanFrameworkBuilder();
    builder.projection(columns);
    builder.providedSchema(schema);
    builder.setUserName(scan.path("userName").asText());
    builder.setReaderFactory(new BasicScanFactory(readers.iterator()));
    builder.nullType(Types.optional(TypeProtos.MinorType.VARCHAR));
    builder.batchRecordLimit(Math.toIntExact(services.options().getLong(org.apache.drill.exec.ExecConstants.NATIVE_SCAN_BATCH_RECORDS.getOptionName())));
    return builder.buildScan();
  }
}
