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
package org.apache.drill.exec.store.hive.writers.primitive;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.vector.complex.writer.VarCharWriter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.io.Text;

public class HiveVarCharWriter extends AbstractSingleValueWriter<HiveVarcharObjectInspector, VarCharWriter> {

  private final DrillBuf drillBuf;

  public HiveVarCharWriter(HiveVarcharObjectInspector inspector, VarCharWriter writer, DrillBuf drillBuf) {
    super(inspector, writer);
    this.drillBuf = drillBuf;
  }

  @Override
  public void write(Object value) {
    Text textValue = inspector.getPrimitiveWritableObject(value).getTextValue();
    drillBuf.setBytes(0, textValue.getBytes());
    writer.writeVarChar(0, textValue.getLength(), drillBuf);
  }

}
