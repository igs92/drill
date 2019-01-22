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
package org.apache.drill.exec.store.hive.writers.complex;

import java.util.Map;
import java.util.Set;

import org.apache.drill.exec.store.hive.writers.HiveValueWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;

public class HiveMapWriter implements HiveValueWriter {

  private final MapObjectInspector mapObjectInspector;

  private final BaseWriter.MapWriter mapWriter;

  public HiveMapWriter(MapObjectInspector mapObjectInspector, BaseWriter.MapWriter mapWriter,
                       HiveValueWriter mapKeyWriter, HiveValueWriter mapValueWriter) {
    // todo: implement
    this.mapObjectInspector = mapObjectInspector;
    this.mapWriter = mapWriter;

  }


  @Override
  public void write(Object value) {

    Map<?, ?> map = mapObjectInspector.getMap(value);
    Set<? extends Map.Entry<?, ?>> entries = map.entrySet();
    mapWriter.start();
    for (Map.Entry<?, ?> entry : entries) {
      Object key = entry.getKey();

    }
    mapWriter.end();

    // todo: implement
    /*
     * Can't be implemented while appropriate MapVector isn't designed and merged into master
     */
  }

}
