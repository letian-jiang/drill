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
package org.apache.drill.exec.store.iceberg;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.Arrays;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.parser.LogicalExpressionParser;
import org.apache.drill.exec.store.iceberg.plan.DrillExprToIcebergTranslator;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.types.Types;
import org.junit.Test;

public class TestIcebergDateTranslation {

  @Test
  public void timeCastsTranslateMillisecondsToIcebergMicroseconds() {
    Schema schema = new Schema(Types.NestedField.optional(1, "t", Types.TimeType.get()));
    for (String expr : Arrays.asList("greater_than(`t`,cast(1001 as TIME))", "greater_than(`t`,cast('00:00:01.001' as TIME))")) {
      Evaluator evaluator = new Evaluator(schema.asStruct(), LogicalExpressionParser.parse(expr).accept(DrillExprToIcebergTranslator.INSTANCE, null), true);
      GenericRecord record = GenericRecord.create(schema);
      record.setField("t", 1001000L);
      assertFalse(evaluator.eval(record));
      record.setField("t", 1001001L);
      assertTrue(evaluator.eval(record));
    }
  }

  @Test
  public void timestampCastsTranslateMillisecondsToIcebergMicroseconds() {
    Schema schema = new Schema(Types.NestedField.optional(1, "ts", Types.TimestampType.withoutZone()));
    for (String expr : Arrays.asList("greater_than(`ts`,cast(1924992000123 as TIMESTAMP))", "greater_than(`ts`,cast('2031-01-01 00:00:00.123' as TIMESTAMP))")) {
      Evaluator evaluator = new Evaluator(schema.asStruct(), LogicalExpressionParser.parse(expr).accept(DrillExprToIcebergTranslator.INSTANCE, null), true);
      GenericRecord record = GenericRecord.create(schema);
      record.setField("ts", 1924992000123000L);
      assertFalse(evaluator.eval(record));
      record.setField("ts", 1924992000123001L);
      assertTrue(evaluator.eval(record));
    }
  }

  @Test
  public void serializedDateCastKeepsBoundaryAndEpochDayUnit() {
    Schema schema = new Schema(Types.NestedField.optional(1, "day", Types.DateType.get()));
    for (String expr : Arrays.asList("less_than_or_equal_to(`day`, cast(904694400000 as DATE))", "less_than_or_equal_to(`day`, cast('1998-09-02' as DATE))")) {
      LogicalExpression expression = LogicalExpressionParser.parse(expr);
      Evaluator evaluator = new Evaluator(schema.asStruct(), expression.accept(DrillExprToIcebergTranslator.INSTANCE, null), true);
      GenericRecord record = GenericRecord.create(schema);
      record.setField("day", Math.toIntExact(java.time.LocalDate.of(1998, 9, 2).toEpochDay()));
      assertTrue(evaluator.eval(record));
      record.setField("day", Math.toIntExact(java.time.LocalDate.of(1998, 9, 3).toEpochDay()));
      assertFalse(evaluator.eval(record));
    }
  }

  @Test
  public void greaterOrEqualUsesRightOperand() {
    Schema schema = new Schema(Types.NestedField.optional(1, "n", Types.IntegerType.get()));
    Evaluator evaluator = new Evaluator(schema.asStruct(), LogicalExpressionParser.parse("greater_than_or_equal_to(`n`, 5)").accept(DrillExprToIcebergTranslator.INSTANCE, null), true);
    GenericRecord record = GenericRecord.create(schema);
    record.setField("n", 4);
    assertFalse(evaluator.eval(record));
    record.setField("n", 5);
    assertTrue(evaluator.eval(record));
  }
}
