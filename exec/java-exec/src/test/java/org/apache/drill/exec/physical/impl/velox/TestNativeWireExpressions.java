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

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.config.LogicalPlanPersistence;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Native plans must retain the expression syntax sent to Java workers.
 */
public class TestNativeWireExpressions {

  @Test
  public void decimalLiteralUsesOriginalWireSemantics() throws Exception {
    var config = DrillConfig.create();
    var mapper = new LogicalPlanPersistence(config, ClassPathScanner.fromPrescan(config)).getMapper();
    var literal = new ValueExpressions.VarDecimalExpression(new java.math.BigDecimal("0.2"), 2, 1, org.apache.drill.common.expression.ExpressionPosition.UNKNOWN);
    String wire = mapper.writeValueAsString(literal);
    LogicalExpression parsed = mapper.readValue(wire, LogicalExpression.class);
    assertTrue(parsed instanceof ValueExpressions.DoubleExpression);
    assertEquals(0.2, ((ValueExpressions.DoubleExpression) parsed).getDouble(), 0);
  }
}
