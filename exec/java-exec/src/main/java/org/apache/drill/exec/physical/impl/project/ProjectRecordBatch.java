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
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ValueVectorReadExpression;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Project;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.SimpleRecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.util.record.RecordBatchStats;
import org.apache.drill.exec.util.record.RecordBatchStats.RecordBatchIOType;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.UntypedNullHolder;
import org.apache.drill.exec.vector.UntypedNullVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProjectRecordBatch extends AbstractSingleRecordBatch<Project> {
  private static final Logger logger = LoggerFactory.getLogger(ProjectRecordBatch.class);

  /**
   * Wraps state associated with physical vectors: the vectors, writers,
   * transfer pairs, and so on. Provides a simple interface to separate
   * the logical planning, in {@link ProjectionMaterializer}, from the
   * physical setup in this class.
   */
  public class VectorState {
    List<TransferPair> transfers = new ArrayList<>();

    public void initComplexWriters() {
      // Lazy initialization of the list of complex writers, if not done yet.
      if (complexWriters == null) {
        complexWriters = new ArrayList<>();
      } else {
        complexWriters.clear();
      }
    }

    public void setupNewSchema(RecordBatch incomingBatch, int configuredBatchSize) {
      transfers.clear();
      memoryManager = new ProjectMemoryManager(configuredBatchSize);
      memoryManager.init(incomingBatch, ProjectRecordBatch.this);
      if (allocationVectors != null) {
        for (ValueVector v : allocationVectors) {
          v.clear();
        }
      }
      allocationVectors = new ArrayList<>();

      if (complexWriters != null) {
        container.clear();
      } else {
        // Release the underlying DrillBufs and reset the ValueVectors to empty
        // Not clearing the container here is fine since Project output schema is
        // not determined solely based on incoming batch. It is defined by the
        // expressions it has to evaluate.
        //
        // If there is a case where only the type of ValueVector already present
        // in container is changed then addOrGet method takes care of it by
        // replacing the vectors.
        container.zeroVectors();
      }
    }

    public void addTransferField(String name, ValueVector vvIn) {
      FieldReference ref = new FieldReference(name);
      ValueVector vvOut = container.addOrGet(MaterializedField.create(ref.getAsNamePart().getName(),
        vvIn.getField().getType()), callBack);
      memoryManager.addTransferField(vvIn, vvIn.getField().getName(), vvOut.getField().getName());
      transfers.add(vvIn.makeTransferPair(vvOut));
    }

    public int addDirectTransfer(FieldReference ref, ValueVectorReadExpression vectorRead, RecordBatch incomingBatch) {
      TypedFieldId id = vectorRead.getFieldId();
      ValueVector vvIn = incomingBatch.getValueAccessorById(id.getIntermediateClass(), id.getFieldIds()).getValueVector();
      Preconditions.checkNotNull(incomingBatch);

      ValueVector vvOut =
          container.addOrGet(MaterializedField.create(ref.getLastSegment().getNameSegment().getPath(),
          vectorRead.getMajorType()), callBack);
      TransferPair tp = vvIn.makeTransferPair(vvOut);
      memoryManager.addTransferField(vvIn, TypedFieldId.getPath(id, incomingBatch), vvOut.getField().getName());
      transfers.add(tp);
      return vectorRead.getFieldId().getFieldIds()[0];
    }

    public ValueVectorWriteExpression addOutputVector(String name, LogicalExpression expr) {
      MaterializedField outputField = MaterializedField.create(name, expr.getMajorType());
      ValueVector vv = container.addOrGet(outputField, callBack);
      allocationVectors.add(vv);
      TypedFieldId fid = container.getValueVectorId(SchemaPath.getSimplePath(outputField.getName()));
      ValueVectorWriteExpression write = new ValueVectorWriteExpression(fid, expr, true);
      memoryManager.addNewField(vv, write);
      return write;
    }

    public void addComplexField(FieldReference ref) {
      if (complexFieldReferencesList == null) {
        complexFieldReferencesList = Lists.newArrayList();
      } else {
        complexFieldReferencesList.clear();
      }

      // save the field reference for later for getting schema when input is empty
      complexFieldReferencesList.add(ref);
      memoryManager.addComplexField(null); // this will just add an estimate to the row width
    }

    public void doAlloc(int recordCount) {
      // Allocate vv in the allocationVectors.
      for (ValueVector v : allocationVectors) {
        AllocationHelper.allocateNew(v, recordCount);
      }

      // Allocate vv for complexWriters.
      if (complexWriters != null) {
        for (ComplexWriter writer : complexWriters) {
          writer.allocate();
        }
      }
    }

    public void setValueCount(int count) {
      if (count == 0) {
        container.setEmpty();
        return;
      }
      for (ValueVector v : allocationVectors) {
        v.getMutator().setValueCount(count);
      }

      // Value counts for vectors should have been set via
      // the transfer pairs or vector copies.
      container.setRecordCount(count);

      if (complexWriters == null) {
        return;
      }

      for (ComplexWriter writer : complexWriters) {
        writer.setValueCount(count);
      }
    }

    public ValueVectorWriteExpression addEvalVector(String outputName,
        LogicalExpression expr, RecordBatch incomingBatch) {
      MaterializedField outputField = MaterializedField.create(outputName, expr.getMajorType());
      ValueVector ouputVector = container.addOrGet(outputField, callBack);
      allocationVectors.add(ouputVector);
      TypedFieldId fid = container.getValueVectorId(SchemaPath.getSimplePath(outputField.getName()));
      boolean useSetSafe = !(ouputVector instanceof FixedWidthVector);
      ValueVectorWriteExpression write = new ValueVectorWriteExpression(fid, expr, useSetSafe);
      memoryManager.addNewField(ouputVector, write);

      // We cannot do multiple transfers from the same vector. However we still
      // need to instantiate the output vector.
      if (expr instanceof ValueVectorReadExpression) {
        ValueVectorReadExpression vectorRead = (ValueVectorReadExpression) expr;
        if (!vectorRead.hasReadPath()) {
          TypedFieldId id = vectorRead.getFieldId();
          ValueVector vvIn = incomingBatch.getValueAccessorById(id.getIntermediateClass(),
                  id.getFieldIds()).getValueVector();
          vvIn.makeTransferPair(ouputVector);
        }
      }
      return write;
    }
  }

  private Projector projector;
  private List<ValueVector> allocationVectors;
  private List<ComplexWriter> complexWriters;
  private List<FieldReference> complexFieldReferencesList;
  private final VectorState vectorState;
  private boolean hasRemainder;
  private int remainderIndex;
  private int recordCount;
  private ProjectMemoryManager memoryManager;
  private boolean first = true;
  private boolean wasNone; // whether a NONE iter outcome was already seen

  public ProjectRecordBatch(Project pop, RecordBatch incoming, FragmentContext context) {
    super(pop, context, incoming);
    vectorState = new VectorState();
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
    memoryManager.update();

    if (first && incomingRecordCount == 0) {
      if (complexWriters != null) {
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
            for (FieldReference fieldReference : complexFieldReferencesList) {
              MaterializedField field = MaterializedField.create(fieldReference.getAsNamePart().getName(),
                      UntypedNullHolder.TYPE);
              container.add(new UntypedNullVector(field, container.getAllocator()));
            }
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
          memoryManager.update();
          logger.trace("doWork():[1] memMgr RC {}, incoming rc {}, incoming {}, Project {}",
                       memoryManager.getOutputRowCount(), incomingRecordCount, incoming, this);
        }
      }
    }

    if (complexWriters != null && getLastKnownOutcome() == EMIT) {
      throw new UnsupportedOperationException("Currently functions producing complex types as output are not " +
        "supported in project list for subquery between LATERAL and UNNEST. Please re-write the query using this " +
        "function in the projection list of outermost query.");
    }

    first = false;
    container.zeroVectors();

    int maxOuputRecordCount = memoryManager.getOutputRowCount();
    logger.trace("doWork():[2] memMgr RC {}, incoming rc {}, incoming {}, project {}",
                 memoryManager.getOutputRowCount(), incomingRecordCount, incoming, this);

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
    if (complexWriters != null) {
      container.buildSchema(SelectionVectorMode.NONE);
    }

    memoryManager.updateOutgoingStats(outputRecords);
    RecordBatchStats.logRecordBatchStats(RecordBatchIOType.OUTPUT, this, getRecordBatchStatsContext());

    // Get the final outcome based on hasRemainder since that will determine if all the incoming records were
    // consumed in current output batch or not
    return getFinalOutcome(hasRemainder);
  }

  private void handleRemainder() {
    int remainingRecordCount = incoming.getRecordCount() - remainderIndex;
    assert memoryManager.incomingBatch() == incoming;
    int recordsToProcess = Math.min(remainingRecordCount, memoryManager.getOutputRowCount());
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
    if (complexWriters != null) {
      container.buildSchema(SelectionVectorMode.NONE);
    }

    memoryManager.updateOutgoingStats(projRecords);
    RecordBatchStats.logRecordBatchStats(RecordBatchIOType.OUTPUT, this, getRecordBatchStatsContext());
  }

  // Called from generated code.

  public void addComplexWriter(ComplexWriter writer) {
    complexWriters.add(writer);
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

    ClassGenerator<Projector> cg = CodeGenerator.getRoot(Projector.TEMPLATE_DEFINITION, context.getOptions());
    ProjectionMaterializer em = new ProjectionMaterializer(context.getOptions(),
        cg, incomingBatch, popConfig.getExprs(), context.getFunctionRegistry(),
        vectorState, unionTypeEnabled);
    em.setup();

    try {
      CodeGenerator<Projector> codeGen = cg.getCodeGenerator();
      codeGen.plainJavaCapable(true);
      // Uncomment out this line to debug the generated code.
      // codeGen.saveCodeForDebugging(true);
      this.projector = context.getImplementationClass(codeGen);
      projector.setup(context, incomingBatch, this, vectorState.transfers);
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
