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
package org.apache.drill.exec.physical.impl.project;

import static org.apache.drill.exec.record.RecordBatch.IterOutcome.EMIT;

import java.io.IOException;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Project;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.SimpleRecordBatch;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.util.record.RecordBatchStats;
import org.apache.drill.exec.util.record.RecordBatchStats.RecordBatchIOType;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProjectRecordBatch extends AbstractSingleRecordBatch<Project> {
  private static final Logger logger = LoggerFactory.getLogger(ProjectRecordBatch.class);

  private final VectorState vectorState;
  private Projector projector;
  private boolean hasRemainder;
  private int remainderIndex;
  private int recordCount;
  private boolean first = true;
  private boolean wasNone; // whether a NONE iter outcome was already seen

  public ProjectRecordBatch(Project pop, RecordBatch incoming, FragmentContext context) {
    super(pop, context, incoming);
    vectorState = new VectorState(this, callBack);
  }

  @Override
  public int getRecordCount() {
    return recordCount;
  }

  @Override
  protected void killIncoming(boolean sendUpstream) {
    super.killIncoming(sendUpstream);
    hasRemainder = false;
  }

  @Override
  public IterOutcome innerNext() {
    if (wasNone) {
      return IterOutcome.NONE;
    }
    recordCount = 0;
    if (hasRemainder) {
      handleRemainder();
      // Check if we are supposed to return EMIT outcome and have consumed entire batch
      return getFinalOutcome(hasRemainder);
    }
    return super.innerNext();
  }

  @Override
  public VectorContainer getOutgoingContainer() {
    return container;
  }

  @Override
  protected IterOutcome doWork() {
    if (wasNone) {
      return IterOutcome.NONE;
    }

    int incomingRecordCount = incoming.getRecordCount();

    logger.trace("doWork(): incoming rc {}, incoming {}, Project {}", incomingRecordCount, incoming, this);
    //calculate the output row count
    vectorState.updateMemory();

    if (first && incomingRecordCount == 0 && vectorState.hasComplexWriters()) {
        IterOutcome next = null;
        while (incomingRecordCount == 0) {
          if (getLastKnownOutcome() == EMIT) {
            throw new UnsupportedOperationException("Currently functions producing complex types as output are not " +
                    "supported in project list for subquery between LATERAL and UNNEST. Please re-write the query using this " +
                    "function in the projection list of outermost query.");
          }

          next = next(incoming);
          setLastKnownOutcome(next);
          if (next == IterOutcome.NONE) {
            // since this is first batch and we already got a NONE, need to set up the schema
            doAlloc(0);
            setValueCount(0);

            // Only need to add the schema for the complex exprs because others should already have
            // been setup during setupNewSchema
            vectorState.addUntypedVectorsForComplexFieldRefs(container);
            container.buildSchema(SelectionVectorMode.NONE);
            wasNone = true;
            return IterOutcome.OK_NEW_SCHEMA;
          } else if (next != IterOutcome.OK && next != IterOutcome.OK_NEW_SCHEMA && next != EMIT) {
            return next;
          } else if (next == IterOutcome.OK_NEW_SCHEMA) {
            try {
              setupNewSchema();
            } catch (SchemaChangeException e) {
              throw new RuntimeException(e);
            }
          }
          incomingRecordCount = incoming.getRecordCount();
          vectorState.updateMemory();
          logger.trace("doWork():[1] memMgr RC {}, incoming rc {}, incoming {}, Project {}",
                       vectorState.getOutputRowCount(), incomingRecordCount, incoming, this);
        }
    }

    if (vectorState.hasComplexWriters() && getLastKnownOutcome() == EMIT) {
      throw new UnsupportedOperationException("Currently functions producing complex types as output are not " +
        "supported in project list for subquery between LATERAL and UNNEST. Please re-write the query using this " +
        "function in the projection list of outermost query.");
    }

    first = false;
    container.zeroVectors();

    int maxOuputRecordCount = vectorState.getOutputRowCount();
    logger.trace("doWork():[2] memMgr RC {}, incoming rc {}, incoming {}, project {}",
        maxOuputRecordCount, incomingRecordCount, incoming, this);

    doAlloc(maxOuputRecordCount);
    long projectStartTime = System.currentTimeMillis();
    int outputRecords = projector.projectRecords(incoming, 0, maxOuputRecordCount, 0);
    long projectEndTime = System.currentTimeMillis();
    logger.trace("doWork(): projection: records {}, time {} ms", outputRecords, (projectEndTime - projectStartTime));

    setValueCount(outputRecords);
    recordCount = outputRecords;
    if (outputRecords < incomingRecordCount) {
      hasRemainder = true;
      remainderIndex = outputRecords;
    } else {
      assert outputRecords == incomingRecordCount;
      incoming.getContainer().zeroVectors();
    }
    // In case of complex writer expression, vectors would be added to batch run-time.
    // We have to re-build the schema.
    if (vectorState.hasComplexWriters()) {
      container.buildSchema(SelectionVectorMode.NONE);
    }

    vectorState.updateOutgoingStats(outputRecords);
    RecordBatchStats.logRecordBatchStats(RecordBatchIOType.OUTPUT, this, getRecordBatchStatsContext());

    // Get the final outcome based on hasRemainder since that will determine if all the incoming records were
    // consumed in current output batch or not
    return getFinalOutcome(hasRemainder);
  }

  private void handleRemainder() {
    int remainingRecordCount = incoming.getRecordCount() - remainderIndex;
    assert vectorState.incomingBatch() == incoming;
    int recordsToProcess = Math.min(remainingRecordCount, vectorState.getOutputRowCount());
    doAlloc(recordsToProcess);

    logger.trace("handleRemainder: remaining RC {}, toProcess {}, remainder index {}, incoming {}, Project {}",
                 remainingRecordCount, recordsToProcess, remainderIndex, incoming, this);

    long projectStartTime = System.currentTimeMillis();
    int projRecords = projector.projectRecords(this.incoming, remainderIndex, recordsToProcess, 0);
    long projectEndTime = System.currentTimeMillis();

    logger.trace("handleRemainder: projection: records {}, time {} ms", projRecords,(projectEndTime - projectStartTime));

    if (projRecords < remainingRecordCount) {
      setValueCount(projRecords);
      recordCount = projRecords;
      remainderIndex += projRecords;
    } else {
      setValueCount(remainingRecordCount);
      hasRemainder = false;
      remainderIndex = 0;
      incoming.getContainer().zeroVectors();
      recordCount = remainingRecordCount;
    }
    // In case of complex writer expression, vectors would be added to batch run-time.
    // We have to re-build the schema.
    if (vectorState.hasComplexWriters()) {
      container.buildSchema(SelectionVectorMode.NONE);
    }

    vectorState.updateOutgoingStats(projRecords);
    RecordBatchStats.logRecordBatchStats(RecordBatchIOType.OUTPUT, this, getRecordBatchStatsContext());
  }

  // Called from generated code.

  public void addComplexWriter(ComplexWriter writer) {
    vectorState.addComplexWriter(writer);
  }

  private void doAlloc(int recordCount) {
    vectorState.doAlloc(recordCount);
  }

  private void setValueCount(int count) {
    vectorState.setValueCount(count);
  }

  private void setupNewSchemaFromInput(RecordBatch incomingBatch) throws SchemaChangeException {
    long setupNewSchemaStartTime = System.currentTimeMillis();
    // get the output batch size from config.
    int configuredBatchSize = (int) context.getOptions().getOption(ExecConstants.OUTPUT_BATCH_SIZE_VALIDATOR);
    vectorState.setupNewSchema(incomingBatch, configuredBatchSize);

    ProjectorCodeGenerator projectorCodeGen = new ProjectorCodeGenerator(context.getOptions(),
        incomingBatch, popConfig.getExprs(), context.getFunctionRegistry(),
        vectorState, unionTypeEnabled);

    projectorCodeGen.setupProjectExpressions();
    try {
      projector = projectorCodeGen.getImplementation(context);
      projector.setup(context, incomingBatch, this, vectorState.getTransfers());
    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException("Failure while attempting to load generated class", e);
    }

    long setupNewSchemaEndTime = System.currentTimeMillis();
      logger.trace("setupNewSchemaFromInput: time {}  ms, Project {}, incoming {}",
                  (setupNewSchemaEndTime - setupNewSchemaStartTime), this, incomingBatch);
  }

  @Override
  protected boolean setupNewSchema() throws SchemaChangeException {
    setupNewSchemaFromInput(incoming);
    if (container.isSchemaChanged() || callBack.getSchemaChangedAndReset()) {
      container.buildSchema(SelectionVectorMode.NONE);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Handle Null input specially when Project operator is for query output. This happens when input return 0 batch
   * (returns a FAST NONE directly).
   *
   * <p>
   * Project operator has to return a batch with schema derived using the following 3 rules:
   * </p>
   * <ul>
   *  <li>Case 1:  *  ==>  expand into an empty list of columns. </li>
   *  <li>Case 2:  regular column reference ==> treat as nullable-int column </li>
   *  <li>Case 3:  expressions => Call ExpressionTreeMaterialization over an empty vector contain.
   *           Once the expression is materialized without error, use the output type of materialized
   *           expression. </li>
   * </ul>
   *
   * <p>
   * The batch is constructed with the above rules, and recordCount = 0.
   * Returned with OK_NEW_SCHEMA to down-stream operator.
   * </p>
   */
  @Override
  protected IterOutcome handleNullInput() {
    if (!popConfig.isOutputProj()) {
      return super.handleNullInput();
    }

    VectorContainer emptyVC = new VectorContainer();
    emptyVC.buildSchema(SelectionVectorMode.NONE);
    RecordBatch emptyIncomingBatch = new SimpleRecordBatch(emptyVC, context);

    try {
      setupNewSchemaFromInput(emptyIncomingBatch);
    } catch (SchemaChangeException e) {
      kill(false);
      logger.error("Failure during query", e);
      context.getExecutorState().fail(e);
      return IterOutcome.STOP;
    }

    doAlloc(0);
    container.buildSchema(SelectionVectorMode.NONE);
    container.setEmpty();
    wasNone = true;
    return IterOutcome.OK_NEW_SCHEMA;
  }

  @Override
  public void dump() {
    logger.error("ProjectRecordBatch[projector={}, hasRemainder={}, remainderIndex={}, recordCount={}, container={}]",
        projector, hasRemainder, remainderIndex, recordCount, container);
  }
}
