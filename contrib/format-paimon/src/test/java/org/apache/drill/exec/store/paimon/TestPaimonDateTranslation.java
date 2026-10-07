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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.List;
import java.util.TimeZone;
import org.apache.drill.common.FunctionNames;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.parser.LogicalExpressionParser;
import org.apache.drill.exec.store.paimon.plan.DrillExprToPaimonTranslator;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.Test;

public class TestPaimonDateTranslation {

  @Test
  public void dateBoundariesDoNotDependOnJvmTimeZone() {
    TimeZone previous = TimeZone.getDefault();
    try {
      RowType rowType = RowType.of(new org.apache.paimon.types.DataType[] { DataTypes.DATE() }, new String[] { "day" });
      for (String zone : List.of("UTC", "America/New_York", "Asia/Singapore")) {
        TimeZone.setDefault(TimeZone.getTimeZone(zone));
        for (long day : new long[] { -1, 0, 10471 }) {
          var expression = new FunctionCall(FunctionNames.LE, List.of(SchemaPath.getSimplePath("day"), ValueExpressions.getDate(day * 86400000L)), ExpressionPosition.UNKNOWN);
          var predicate = DrillExprToPaimonTranslator.translate(expression, rowType);
          assertTrue(predicate.test(GenericRow.of((int) day)));
          assertFalse(predicate.test(GenericRow.of((int) day + 1)));
        }
      }
    } finally {
      TimeZone.setDefault(previous);
    }
  }

  @Test
  public void serializedDateCastsKeepBoundaryAndUnits() {
    RowType rowType = RowType.of(new org.apache.paimon.types.DataType[] { DataTypes.DATE() }, new String[] { "day" });
    for (String text : List.of("less_than_or_equal_to(`day`, cast(0 as DATE))", "less_than_or_equal_to(`day`, cast('1970-01-01' as DATE))")) {
      var predicate = DrillExprToPaimonTranslator.translate(LogicalExpressionParser.parse(text), rowType);
      assertTrue(predicate.test(GenericRow.of(0)));
      assertFalse(predicate.test(GenericRow.of(1)));
    }
  }
}
