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
package org.apache.drill.exec.physical.impl.velox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import java.util.Map;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.PhysicalPlanReader;

/**
 * Keeps the original minor plan; adds declarative plugin scan work only.
 */
public final class NativeMinorPlanSerializer {

  private NativeMinorPlanSerializer() {
  }

  public static String serialize(PhysicalPlanReader reader, PhysicalOperator root) throws JsonProcessingException {
    // Preserve the original wire expression syntax used by Java workers.
    // Adding Decimal casts here changes AVG predicate semantics (TPC-H Q17).
    // ObjectMapper.copy() starts with empty serializer caches. Repeating it for
    // every minor makes each query rediscover the same operator/type metadata.
    var mapper = reader.nativeTransportMapper();
    Map<Integer, PhysicalOperator> operators = new HashMap<>();
    collect(root, operators);
    JsonNode tree = mapper.valueToTree(root);
    decorate(tree, operators, mapper);
    return mapper.writeValueAsString(tree);
  }

  private static void collect(PhysicalOperator operator, Map<Integer, PhysicalOperator> operators) {
    operators.put(operator.getOperatorId(), operator);
    for (PhysicalOperator child : operator) {
      collect(child, operators);
    }
  }

  private static void decorate(JsonNode node, Map<Integer, PhysicalOperator> operators, com.fasterxml.jackson.databind.ObjectMapper mapper) throws JsonProcessingException {
    if (node.isObject() && node.has("@id") && node.has("pop")) {
      var operator = operators.get(node.get("@id").asInt());
      if (operator instanceof org.apache.drill.exec.nativeexecution.scan.JniScanProvider) {
        var descriptor = ((org.apache.drill.exec.nativeexecution.scan.JniScanProvider) operator).jniScan(mapper, ((ObjectNode) node).deepCopy());
        ((ObjectNode) node).set("jniScan", descriptor);
      } else if (operator instanceof org.apache.drill.exec.physical.base.SubScan) {
        // The original scan creator is the default bridge. Plugins do not need
        // to implement a native-specific interface to keep using their reader.
        var descriptor = mapper.createObjectNode();
        descriptor.put("provider", "drill-java-subscan");
        descriptor.set("scan", ((ObjectNode) node).deepCopy());
        ((ObjectNode) node).set("jniScan", descriptor);
      }
    }
    var entries = node.fields();
    while (entries.hasNext()) {
      var entry = entries.next();
      if (!entry.getKey().equals("jniScan")) {
        var child = entry.getValue();
        if (child.isArray()) {
          for (var item : child) {
            decorate(item, operators, mapper);
          }
        } else if (child.isObject()) {
          decorate(child, operators, mapper);
        }
      }
    }
  }
}
