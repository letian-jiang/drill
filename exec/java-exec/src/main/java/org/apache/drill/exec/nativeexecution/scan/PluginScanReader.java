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

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Collections;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.scan.ScanOperatorExec;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.util.ImpersonationUtil;

/**
 * Uniform view over the original ScanBatch, scan framework v2/v3, or a specialized bridge.
 */
interface PluginScanReader extends AutoCloseable {

  BatchSchema schema();

  Iterable<VectorWrapper<?>> vectors();

  int rows();

  boolean next();

  void close() throws Exception;

  static PluginScanReader open(JsonNode descriptor, ScanServices resources) throws Exception {
    resources.checkContinue();
    if (!"drill-java-subscan".equals(descriptor.path("provider").asText())) {
      var factory = (PluginScanFactory) Class.forName(descriptor.get("provider").asText()).getDeclaredConstructor().newInstance();
      ScanOperatorExec scan = factory.open(descriptor.get("scan"), resources);
      try {
        resources.checkContinue();
        scan.bind(resources);
        scan.buildSchema();
        resources.checkContinue();
      } catch (Throwable error) {
        org.apache.drill.common.AutoCloseables.close(error, (AutoCloseable) scan::close);
        throw error;
      }
      return new PluginScanReader() {

        public BatchSchema schema() {
          return scan.batchAccessor().schema();
        }

        public Iterable<VectorWrapper<?>> vectors() {
          return scan.batchAccessor().container();
        }

        public int rows() {
          return scan.batchAccessor().rowCount();
        }

        public boolean next() {
          return scan.next();
        }

        public void close() throws Exception {
          scan.close();
        }
      };
    }
    PhysicalOperator operator = resources.planReader().readFragmentLeaf(descriptor.get("scan").toString());
    if (!(operator instanceof SubScan) || operator.iterator().hasNext()) {
      throw new IllegalArgumentException("JNI generic reader accepts only an original leaf SubScan");
    }
    var context = new ScanContext(resources);
    resources.attach(operator, context);
    @SuppressWarnings("unchecked")
    var creator = (BatchCreator<PhysicalOperator>) resources.creators().getOperatorCreator(operator.getClass());
    CloseableRecordBatch scan;
    if (context.isImpersonationEnabled()) {
      scan = ImpersonationUtil.createProxyUgi(operator.getUserName(), context.getQueryUserName()).doAs((java.security.PrivilegedExceptionAction<CloseableRecordBatch>) () -> creator.getBatch(context, operator, Collections.emptyList()));
    } else {
      scan = creator.getBatch(context, operator, Collections.emptyList());
    }
    PluginScanReader reader = new PluginScanReader() {

      public BatchSchema schema() {
        return scan.getSchema();
      }

      public Iterable<VectorWrapper<?>> vectors() {
        return scan;
      }

      public int rows() {
        return scan.getRecordCount();
      }

      public boolean next() {
        context.getExecutorState().checkContinue();
        IterOutcome outcome = scan.next();
        if (outcome == IterOutcome.NONE) {
          return false;
        }
        if (outcome != IterOutcome.OK && outcome != IterOutcome.OK_NEW_SCHEMA) {
          throw new IllegalStateException("Unsupported original plugin scan outcome " + outcome);
        }
        context.getExecutorState().checkContinue();
        return true;
      }

      public void close() throws Exception {
        scan.close();
      }
    };
    try {
      // Preserve its first data batch for ScanHost's first read.
      reader.next();
      if (reader.schema() == null) {
        throw new IllegalStateException("Plugin returned no schema");
      }
      return reader;
    } catch (Throwable error) {
      org.apache.drill.common.AutoCloseables.close(error, reader);
      throw error;
    }
  }
}
