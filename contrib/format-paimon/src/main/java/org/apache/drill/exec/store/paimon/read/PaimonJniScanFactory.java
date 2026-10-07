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
package org.apache.drill.exec.store.paimon.read;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.nativeexecution.scan.PluginScanFactory;
import org.apache.drill.exec.nativeexecution.scan.ScanServices;
import org.apache.drill.exec.physical.impl.scan.ScanOperatorExec;
import org.apache.drill.exec.physical.impl.scan.v3.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.v3.ReaderFactory;
import org.apache.drill.exec.physical.impl.scan.v3.ScanLifecycleBuilder;
import org.apache.drill.exec.physical.impl.scan.v3.SchemaNegotiator;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.paimon.PaimonWork;
import org.apache.drill.exec.store.paimon.format.PaimonFormatPluginConfig;
import org.apache.hadoop.conf.Configuration;

/**
 * Reuses the actual Drill Paimon reader with extracted scan resources.
 */
public final class PaimonJniScanFactory implements PluginScanFactory {

  @Override
  public ScanOperatorExec open(JsonNode scan, ScanServices services) throws Exception {
    var mapper = services.mapper();
    Configuration fs = new Configuration();
    var properties = scan.path("fsConf").fields();
    while (properties.hasNext()) {
      var property = properties.next();
      fs.set(property.getKey(), property.getValue().asText());
    }
    var format = mapper.treeToValue(scan.get("format"), PaimonFormatPluginConfig.class);
    List<SchemaPath> columns = new ArrayList<>();
    for (var column : scan.get("columns")) {
      columns.add(mapper.treeToValue(column, SchemaPath.class));
    }
    List<PaimonWork> works = new ArrayList<>();
    for (var work : scan.get("workList")) {
      works.add(mapper.treeToValue(work, PaimonWork.class));
    }
    var condition = scan.hasNonNull("condition") ? mapper.treeToValue(scan.get("condition"), LogicalExpression.class) : null;
    var schema = scan.hasNonNull("schema") ? mapper.treeToValue(scan.get("schema"), TupleMetadata.class) : null;
    var iterator = works.iterator();
    ScanLifecycleBuilder builder = new ScanLifecycleBuilder();
    builder.options(services.options());
    builder.projection(columns);
    builder.providedSchema(schema);
    builder.userName(scan.path("userName").asText());
    builder.batchRecordLimit(Math.toIntExact(services.options().getLong(org.apache.drill.exec.ExecConstants.NATIVE_SCAN_BATCH_RECORDS.getOptionName())));
    builder.readerFactory(new ReaderFactory<SchemaNegotiator>() {

      public boolean hasNext() {
        return iterator.hasNext();
      }

      public ManagedReader next(SchemaNegotiator negotiator) {
        return new PaimonRecordReader(fs, format, scan.get("path").asText(), columns, condition, iterator.next(), scan.get("maxRecords").asInt(), negotiator);
      }
    });
    return builder.buildScan();
  }
}
