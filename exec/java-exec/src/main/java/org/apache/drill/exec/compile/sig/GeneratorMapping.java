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
package org.apache.drill.exec.compile.sig;

import org.apache.drill.exec.expr.ClassGenerator.BlockType;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

import java.util.EnumMap;
import java.util.Map;

/**
 * The code generator works with four conceptual methods which can
 * have any actual names. This class identify which conceptual methods
 * are in use and their actual names. Callers obtain the method
 * names generically using the {@link BlockType} enum. There is,
 * however, no way to check which methods are in use; the user of
 * this method must already know this information from another
 * source.
 * <table>
 * <tr><th>Conceptual Method</th>
 * <th>BlockType</th>
 * <th>Typical Drill Name</th></tr>
 * <tr><td>setup</td><td>SETUP</td><td>doSetup</td></tr>
 * <tr><td>eval</td><td>EVAL</td><td>doEval</td></tr>
 * <tr><td>reset</td><td>RESET</td><td>?</td></tr>
 * <tr><td>cleanup</td><td>CLEANUP</td><td>?</td></tr>
 * </table>
 */
public class GeneratorMapping {

  private final Map<BlockType, String> map;

  private GeneratorMapping() {
    map = new EnumMap<>(BlockType.class);
  }

  /**
   * Start here to create new mapping, then proceed with chaining, like:
   * {@code GeneratorMapping.methods().setup("doSetup").eval("doEval") ... }
   */
  public static GeneratorMapping methods() {
    return new GeneratorMapping();
  }

  /**
   * Get mapped method name for BlockType
   *
   * @param type block type
   * @return mapped method name
   */
  public String getMethodName(BlockType type) {
    return Preconditions.checkNotNull(map.get(type),
        "The current mapping does not have a %s method defined.",
        type.name().toLowerCase());
  }

  public GeneratorMapping setup(String methodName) {
    return put(BlockType.SETUP, methodName);
  }

  public GeneratorMapping eval(String methodName) {
    return put(BlockType.EVAL, methodName);
  }

  public GeneratorMapping reset(String methodName) {
    return put(BlockType.RESET, methodName);
  }

  public GeneratorMapping cleanup(String methodName) {
    return put(BlockType.CLEANUP, methodName);
  }

  public GeneratorMapping copy() {
    GeneratorMapping copy = new GeneratorMapping();
    copy.map.putAll(map);
    return copy;
  }

  private GeneratorMapping put(BlockType type, String methodName) {
    map.put(type, methodName);
    return this;
  }
}
