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

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.expr.ValueVectorReadExpression;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.SchemaChangeCallBack;
import org.apache.drill.exec.vector.UntypedNullHolder;
import org.apache.drill.exec.vector.UntypedNullVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

/**
 * Wraps state associated with physical vectors: the vectors, writers,
 * transfer pairs, and so on. Provides a simple interface to separate
 * the logical planning, in {@link ProjectorCodeGenerator}, from the
 * physical setup in this class.
 */
public class VectorState {

  private final ProjectRecordBatch projectBatch;

  private final List<TransferPair> transfers = new ArrayList<>();

  private List<ValueVector> allocationVectors;
  private List<BaseWriter.ComplexWriter> complexWriters;
  private List<FieldReference> complexFieldReferences;
  private ProjectMemoryManager memoryManager;
  private final VectorContainer container;
  private final SchemaChangeCallBack callBack;

  public VectorState(ProjectRecordBatch projectBatch, SchemaChangeCallBack callBack) {
    this.projectBatch = projectBatch;
    this.container = projectBatch.getContainer();
    this.callBack = callBack;
  }

  public List<TransferPair> getTransfers() {
    return transfers;
  }

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
    memoryManager.init(incomingBatch, projectBatch);
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
    if (complexFieldReferences == null) {
      complexFieldReferences = Lists.newArrayList();
    } else {
      complexFieldReferences.clear();
    }

    // save the field reference for later for getting schema when input is empty
    complexFieldReferences.add(ref);
    memoryManager.addComplexField(null); // this will just add an estimate to the row width
  }

  public void doAlloc(int recordCount) {
    // Allocate vv in the allocationVectors.
    for (ValueVector v : allocationVectors) {
      AllocationHelper.allocateNew(v, recordCount);
    }

    // Allocate vv for complexWriters.
    if (complexWriters != null) {
      for (BaseWriter.ComplexWriter writer : complexWriters) {
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

    for (BaseWriter.ComplexWriter writer : complexWriters) {
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

  public boolean hasComplexWriters() {
    return complexWriters != null;
  }

  public void addComplexWriter(BaseWriter.ComplexWriter writer) {
    complexWriters.add(writer);
  }

  public void addUntypedVectorsForComplexFieldRefs(VectorContainer container) {
    BufferAllocator allocator = container.getAllocator();
    complexFieldReferences.stream()
        .map(ref -> ref.getAsNamePart().getName())
        .map(name -> MaterializedField.create(name, UntypedNullHolder.TYPE))
        .map(field -> new UntypedNullVector(field, allocator))
        .forEach(container::add);
  }

  public void updateMemory() {
    memoryManager.update();
  }

  public int getOutputRowCount() {
    return memoryManager.getOutputRowCount();
  }

  public void updateOutgoingStats(int outputRecords) {
    memoryManager.updateOutgoingStats(outputRecords);
  }

  public RecordBatch incomingBatch() {
    return memoryManager.incomingBatch();
  }
}
