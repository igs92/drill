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

import org.apache.drill.exec.store.hive.writers.HiveValueWriter;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class HiveStructWriter implements HiveValueWriter {

  private final StructObjectInspector structObjectInspector;

  private final StructField[] structFields;

  private final HiveValueWriter[] fieldWriters;

  public HiveStructWriter(StructObjectInspector structObjectInspector, StructField[] structFields, HiveValueWriter[] fieldWriters) {
    this.structObjectInspector = structObjectInspector;
    this.structFields = structFields;
    this.fieldWriters = fieldWriters;
  }

  @Override
  public void write(Object value) {
    for (int fieldIdx = 0; fieldIdx < structFields.length; fieldIdx++) {
      Object fieldValue = structObjectInspector.getStructFieldData(value, structFields[fieldIdx]);
      if (fieldValue == null) {
        fieldWriters[fieldIdx].writeNull();
      } else {
        fieldWriters[fieldIdx].write(fieldValue);
      }
    }
  }

}
