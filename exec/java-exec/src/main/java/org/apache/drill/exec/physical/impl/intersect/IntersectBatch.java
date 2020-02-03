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
package org.apache.drill.exec.physical.impl.intersect;

import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Intersect;
import org.apache.drill.exec.record.AbstractBinaryRecordBatch;
import org.apache.drill.exec.record.RecordBatch;

public class IntersectBatch extends AbstractBinaryRecordBatch<Intersect> {
  protected IntersectBatch(Intersect popConfig, FragmentContext context,
                           RecordBatch left, RecordBatch right) throws OutOfMemoryException {
    super(popConfig, context, left, right);
  }

  @Override
  public IterOutcome innerNext() {
    return null;
  }

  @Override
  protected void killIncoming(boolean sendUpstream) {

  }

  @Override
  public void dump() {

  }

  @Override
  public int getRecordCount() {
    return 0;
  }
}
