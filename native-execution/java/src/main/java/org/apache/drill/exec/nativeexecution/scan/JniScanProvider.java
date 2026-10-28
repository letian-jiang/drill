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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Supplies plugin-private read work, never a fragment or a compute plan.
 */
public interface JniScanProvider {

  /**
   * The optional descriptor field {@code independentWorkList} names an array
   * inside {@code scan}. Declaring it promises that each element can be opened
   * in an independent reader, with identical schema and no shared cursor or
   * cross-work state. Native drivers consume each assigned work exactly once.
   * Omit it for plugins requiring a single reader for the complete descriptor.
   */
  ObjectNode jniScan(ObjectMapper mapper, ObjectNode originalScan);
}
