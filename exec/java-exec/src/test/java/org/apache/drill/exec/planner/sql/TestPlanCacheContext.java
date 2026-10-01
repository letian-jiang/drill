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
package org.apache.drill.exec.planner.sql;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.planner.logical.DrillTableSelection;
import org.apache.drill.exec.store.PlanCacheTable;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.plan.rel.PluginDrillTable;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestPlanCacheContext {
  @Test
  public void parameterizerKeepsQueriesWithoutLiterals() throws Exception {
    SqlNode query = SqlParser.create("SELECT id FROM custom.items").parseQuery();
    PlanCacheParameterizer.Candidate candidate = PlanCacheParameterizer.parameterize(query);
    assertSame(query, candidate.sql);
    assertTrue(candidate.literals.isEmpty());
  }

  @Test
  public void parameterizerKeepsDateLiteralInTemplate() throws Exception {
    SqlNode query = SqlParser.create("SELECT DATE '2020-01-01', 1.50, 7").parseQuery();
    PlanCacheParameterizer.Candidate candidate = PlanCacheParameterizer.parameterize(query);
    assertTrue(candidate.template.contains("2020-01-01"));
    org.junit.Assert.assertEquals(2, candidate.literals.size());
  }

  @Test
  public void genericStoragePluginChecksOpaqueTableVersion() throws Exception {
    StoragePlugin plugin = mock(StoragePlugin.class);
    StoragePluginConfig firstConfig = mock(StoragePluginConfig.class);
    StoragePluginConfig changedConfig = mock(StoragePluginConfig.class);
    StoragePluginRegistry registry = mock(StoragePluginRegistry.class);
    DrillTableSelection selection = () -> "custom-selection";
    when(plugin.supportPlanCache()).thenReturn(true);
    when(plugin.getConfig()).thenReturn(firstConfig);
    when(plugin.planCacheTable(selection)).thenReturn(new PlanCacheTable("opaque-id", "v1"));
    when(registry.encode(firstConfig)).thenReturn("first-config");
    when(registry.encode(changedConfig)).thenReturn("changed-config");

    SchemaPlus root = mock(SchemaPlus.class);
    SchemaPlus schema = mock(SchemaPlus.class);
    when(root.getSubSchema("custom")).thenReturn(schema);
    PluginDrillTable table = new PluginDrillTable(
        plugin, "custom", "user", selection, Convention.NONE);
    when(schema.getTable("ITEMS")).thenReturn(table);

    PlanCache.ContextSnapshot context = PlanCache.ContextSnapshot.resolve(root,
        SqlParser.create("SELECT * FROM custom.items").parseQuery(), registry);
    assertNotNull(context);
    context = context.withOptionsFingerprint("options-v1");
    verify(plugin).planCacheTable(selection);

    when(registry.getPlugin("custom")).thenReturn(plugin);
    when(plugin.planCacheTableVersion("opaque-id")).thenReturn("v1");
    assertFalse(context.isCurrent("options-v2", registry));
    assertTrue(context.isCurrent("options-v1", registry));
    when(plugin.getConfig()).thenReturn(changedConfig);
    assertFalse(context.isCurrent("options-v1", registry));
    when(plugin.getConfig()).thenReturn(firstConfig);
    assertTrue(context.isCurrent("options-v1", registry));
    when(plugin.planCacheTableVersion("opaque-id")).thenReturn("v2");
    assertFalse(context.isCurrent("options-v1", registry));
  }
}
