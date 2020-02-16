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
package org.apache.drill.exec.physical.impl.aggregate;

import java.util.List;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.compile.TemplateClassDefinition;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.config.HashAggregate;
import org.apache.drill.exec.physical.impl.common.HashTableConfig;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;

// todo: javadoc ?
public interface HashAggregator {
  TemplateClassDefinition<HashAggregator> TEMPLATE_DEFINITION =
      new TemplateClassDefinition<>(HashAggregator.class, HashAggTemplate.class);

  // todo: javadoc ?
  enum AggOutcome {
    RETURN_OUTCOME,
    CLEANUP_AND_RETURN,
    UPDATE_AGGREGATOR,
    CALL_WORK_AGAIN
  }

  /**
   * Enumerates possible results for {@link HashAggregator#outputCurrentBatch()}
   */
  enum AggIterOutcome {
    /**
     * Batch returned
     */
    AGG_OK,
    /**
     * End of data
     */
    AGG_NONE,
    /**
     * Call again
     */
    AGG_RESTART,
    /**
     * Like ok but emit
     */
    AGG_EMIT
  }

  void setup(HashAggregate hashAggrConfig, HashTableConfig htConfig, FragmentContext context,
             OperatorContext oContext, RecordBatch incoming, HashAggBatch outgoing,
             LogicalExpression[] valueExprs, List<TypedFieldId> valueFieldIds, ClassGenerator<?> cg,
             TypedFieldId[] keyFieldIds, VectorContainer outContainer, int extraRowBytes);
  // todo: javadoc ?
  IterOutcome getOutcome();
  // todo: javadoc ?
  int getOutputCount();
  // todo: javadoc ?
  AggOutcome doWork();
  // todo: javadoc ?
  void cleanup();
  // todo: javadoc ?
  boolean allFlushed();
  // todo: javadoc ?
  boolean buildComplete();
  // todo: javadoc ?
  boolean handlingEmit();
  // todo: javadoc ?
  AggIterOutcome outputCurrentBatch();
  // todo: javadoc ?
  boolean earlyOutput();
  // todo: javadoc ?
  RecordBatch getNewIncoming();
  // todo: javadoc ?
  void adjustOutputCount(int outputBatchSize, int oldRowWidth, int newRowWidth);
}
