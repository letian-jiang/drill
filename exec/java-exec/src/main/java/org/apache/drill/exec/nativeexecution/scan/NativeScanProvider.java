/* Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0 . */
package org.apache.drill.exec.nativeexecution.scan;

import org.apache.drill.exec.physical.impl.velox.NativeColumnarBridge;
import java.util.List;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.record.MaterializedField;

/**
 * Plugin supplies immutable scan work and metadata; native execution never advances a Java reader.
 */
public interface NativeScanProvider {

  /**
   * Null selects the existing Java plugin reader before execution starts.
   */
  Descriptor nativeScan();

  final class Descriptor {

    public final String provider;

    public final int version;

    public final List<MaterializedField> readFields;

    public final List<MaterializedField> outputFields;

    public final LogicalExpression filter;

    public final String json;

    public Descriptor(List<MaterializedField> readFields, List<MaterializedField> outputFields, LogicalExpression filter, String json) {
      this(null, 1, readFields, outputFields, filter, json);
    }

    /**
     * Provider matches NativeScanPlugin.provider(); json contains its immutable work payload.
     */
    public Descriptor(String provider, int version, List<MaterializedField> readFields, List<MaterializedField> outputFields, LogicalExpression filter, String json) {
      if (version < 1 || (provider != null && provider.isBlank())) {
        throw new IllegalArgumentException("Invalid native scan provider/version");
      }
      this.provider = provider;
      this.version = version;
      this.readFields = List.copyOf(readFields);
      this.outputFields = List.copyOf(outputFields);
      this.filter = filter;
      this.json = json;
      NativeColumnarBridge.fields(this.readFields);
      NativeColumnarBridge.fields(this.outputFields);
    }
  }
}
