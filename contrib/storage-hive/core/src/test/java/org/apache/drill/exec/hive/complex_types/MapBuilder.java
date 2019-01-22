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
package org.apache.drill.exec.hive.complex_types;

import java.util.HashMap;
import java.util.Map;

/**
 * Useful builder for concise creation of HashMaps with data.
 * Example usage:
 * <code>
 *   Map&lt;Integer, Integer&gt; mapIntToInt = MapBuilder.startMap(1, 10).put(2, 20).put(3, 30).get();
 * </code>
 *
 * @param <K>
 * @param <V>
 */
public class MapBuilder<K, V> {

  /**
   * Map instance created when builder initialized
   */
  private final Map<K, V> map = new HashMap<>();

  /**
   * Init builder and add first Map.Entry to builder's map
   *
   * @param key - first entry key
   * @param val - first entry value
   * @param <KT> - key type
   * @param <VT> - value type
   * @return - new builder instance with map containing key and value
   */
  public static <KT, VT> MapBuilder<KT, VT> startMap(KT key, VT val) {
    return new MapBuilder<KT, VT>().put(key, val);
  }

  public static <KT, VT> MapBuilder<KT, VT> startMap() {
    return new MapBuilder<>();
  }

  /**
   * Put key and value into underlying map
   *
   * @param key key
   * @param val value
   * @return the builder instance
   */
  public MapBuilder<K, V> put(K key, V val) {
    map.put(key, val);
    return this;
  }

  /**
   * Get map with data
   * @return map
   */
  public Map<K, V> get() {
    return map;
  }

}
