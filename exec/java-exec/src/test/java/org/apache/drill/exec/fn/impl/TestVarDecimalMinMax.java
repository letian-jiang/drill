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
package org.apache.drill.exec.fn.impl;

import java.math.BigDecimal;
import org.apache.drill.test.BaseTestQuery;
import org.junit.Test;

public class TestVarDecimalMinMax extends BaseTestQuery {

  private void verifyShrinkingState(boolean nullable) throws Exception {
    // BigInteger encodings shrink from three bytes to two. A later value must
    // compare against the logical state value, even when its array is larger.
    String values = "('994.97', '-994.97'), ('302.02', '-302.02'), " + "('481.76', '-481.76'), ('382.09', '-382.09')" + (nullable ? ", (NULL, NULL)" : "");
    testBuilder().sqlQuery("SELECT MIN(CAST(lo AS DECIMAL(15, 2))) AS lo, " + "MAX(CAST(hi AS DECIMAL(15, 2))) AS hi FROM (VALUES " + values + ") AS t(lo, hi)").ordered().baselineColumns("lo", "hi").baselineValues(new BigDecimal("302.02"), new BigDecimal("-302.02")).go();
  }

  @Test
  public void requiredStateShrinks() throws Exception {
    verifyShrinkingState(false);
  }

  @Test
  public void nullableStateShrinks() throws Exception {
    verifyShrinkingState(true);
  }

  @Test
  public void groupedStateAndNulls() throws Exception {
    for (boolean hash : new boolean[] { true, false }) {
      setSessionOption("planner.enable_hashagg", hash);
      try {
        testBuilder().sqlQuery("SELECT g, MIN(CAST(lo AS DECIMAL(15, 2))) AS lo, " + "MAX(CAST(hi AS DECIMAL(15, 2))) AS hi FROM (VALUES " + "(1, '994.97', '-994.97'), (1, '302.02', '-302.02'), " + "(1, '481.76', '-481.76'), (1, NULL, NULL), " + "(2, '9999999999999.99', '-9999999999999.99'), " + "(2, '0.01', '-0.01'), (2, '10.00', '-10.00'), " + "(3, NULL, NULL)) AS t(g, lo, hi) GROUP BY g").unOrdered().baselineColumns("g", "lo", "hi").baselineValues(1, new BigDecimal("302.02"), new BigDecimal("-302.02")).baselineValues(2, new BigDecimal("0.01"), new BigDecimal("-0.01")).baselineValues(3, null, null).go();
      } finally {
        resetSessionOption("planner.enable_hashagg");
      }
    }
  }
}
