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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.Collections;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.junit.Test;

public class TestIcebergScanStats {

  @Test
  public void splitTasksDoNotMultiplyFileRecordCount() {
    DataFile file = mock(DataFile.class);
    when(file.recordCount()).thenReturn(1000L);
    when(file.fileSizeInBytes()).thenReturn(1000L);
    FileScanTask a = mock(FileScanTask.class);
    FileScanTask b = mock(FileScanTask.class);
    when(a.file()).thenReturn(file);
    when(b.file()).thenReturn(file);
    when(a.length()).thenReturn(100L);
    when(b.length()).thenReturn(900L);
    CombinedScanTask first = mock(CombinedScanTask.class);
    CombinedScanTask second = mock(CombinedScanTask.class);
    when(first.files()).thenReturn(Collections.singletonList(a));
    when(second.files()).thenReturn(Collections.singletonList(b));
    assertEquals(1000.0, IcebergGroupScan.estimateRecords(java.util.Arrays.asList(new IcebergCompleteWork(null, first), new IcebergCompleteWork(null, second))), 0.0);
    assertEquals(100.0, IcebergGroupScan.estimateRecords(Collections.singletonList(new IcebergCompleteWork(null, first))), 0.0);
  }
}
