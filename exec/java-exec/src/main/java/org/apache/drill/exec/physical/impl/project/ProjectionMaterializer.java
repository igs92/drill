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

import java.util.HashMap;
import java.util.List;

import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.drill.common.expression.ConvertExpression;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.FunctionCallFactory;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.expression.PathSegment.NameSegment;
import org.apache.drill.common.expression.fn.FunctionReplacementUtils;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.types.Types;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.DrillFuncHolderExpr;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.ValueVectorReadExpression;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.expr.fn.FunctionLookupContext;
import org.apache.drill.exec.physical.impl.project.ProjectRecordBatch.VectorState;
import org.apache.drill.exec.planner.StarColumnHelper;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.ColumnExplorer;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;

import com.carrotsearch.hppc.IntHashSet;

/**
 * Plans the projection given the incoming and requested outgoing schemas. Works
 * with the {@link VectorState} to create required vectors, writers and so on.
 * Populates the code generator with the "projector" expressions.
 */
class ProjectionMaterializer {

  private static final String EMPTY_STRING = "";

  private static class ClassifierResult {
    private boolean isStar;
    private List<String> outputNames;
    private String prefix = "";
    private final HashMap<String, Integer> prefixMap = Maps.newHashMap();
    private final CaseInsensitiveMap outputMap = new CaseInsensitiveMap();
    private final CaseInsensitiveMap sequenceMap = new CaseInsensitiveMap();

    private void clear() {
      isStar = false;
      prefix = "";
      if (outputNames != null) {
        outputNames.clear();
      }

      // note: don't clear the internal maps since they have cumulative data..
    }
  }

  private final ClassGenerator<Projector> cg;
  private final RecordBatch incomingBatch;
  private final List<NamedExpression> exprSpec;
  private final FunctionLookupContext functionLookupContext;
  private final VectorState vectorState;
  private final boolean unionTypeEnabled;
  private final ErrorCollector collector = new ErrorCollectorImpl();
  private final ColumnExplorer columnExplorer;
  private final IntHashSet transferFieldIds = new IntHashSet();
  private final ProjectionMaterializer.ClassifierResult result = new ClassifierResult();
  private boolean isAnyWildcard;
  private boolean classify;

  public ProjectionMaterializer(OptionManager options,
      ClassGenerator<Projector> cg, RecordBatch incomingBatch,
      List<NamedExpression> exprSpec,
      FunctionLookupContext functionLookupContext, VectorState vectorState,
      boolean unionTypeEnabled) {
    this.cg = cg;
    this.incomingBatch = incomingBatch;
    this.exprSpec = exprSpec;
    this.functionLookupContext = functionLookupContext;
    this.vectorState = vectorState;
    this.unionTypeEnabled = unionTypeEnabled;
    columnExplorer = new ColumnExplorer(options);
  }

  public void setup() throws SchemaChangeException {
    List<NamedExpression> exprs = exprSpec != null ? exprSpec
        : inferExpressions();
    isAnyWildcard = isAnyWildcard(exprs);
    classify = isClassificationNeeded(exprs);

    for (NamedExpression namedExpression : exprs) {
      setupExpression(namedExpression);
    }
  }

  private List<NamedExpression> inferExpressions() {
    List<NamedExpression> exprs = Lists.newArrayList();
    for (MaterializedField field : incomingBatch.getSchema()) {
      String fieldName = field.getName();
      if (Types.isComplex(field.getType())
          || Types.isRepeated(field.getType())) {
        LogicalExpression convertToJson = FunctionCallFactory.createConvert(
            ConvertExpression.CONVERT_TO, "JSON",
            SchemaPath.getSimplePath(fieldName), ExpressionPosition.UNKNOWN);
        String castFuncName = FunctionReplacementUtils
            .getCastFunc(MinorType.VARCHAR);
        List<LogicalExpression> castArgs = Lists.newArrayList();
        castArgs.add(convertToJson); // input_expr
        // Implicitly casting to varchar, since we don't know actual source
        // length, cast to undefined length, which will preserve source length
        castArgs.add(new ValueExpressions.LongExpression(
            Types.MAX_VARCHAR_LENGTH, null));
        FunctionCall castCall = new FunctionCall(castFuncName, castArgs,
            ExpressionPosition.UNKNOWN);
        exprs.add(new NamedExpression(castCall, new FieldReference(fieldName)));
      } else {
        exprs.add(new NamedExpression(SchemaPath.getSimplePath(fieldName),
            new FieldReference(fieldName)));
      }
    }
    return exprs;
  }

  private boolean isAnyWildcard(List<NamedExpression> exprs) {
    for (NamedExpression e : exprs) {
      if (isWildcard(e)) {
        return true;
      }
    }
    return false;
  }

  private boolean isWildcard(NamedExpression ex) {
    if (!(ex.getExpr() instanceof SchemaPath)) {
      return false;
    }
    NameSegment expr = ((SchemaPath) ex.getExpr()).getRootSegment();
    return expr.getPath().contains(SchemaPath.DYNAMIC_STAR);
  }

  private boolean isClassificationNeeded(List<NamedExpression> exprs) {
    boolean needed = false;
    for (NamedExpression ex : exprs) {
      if (!(ex.getExpr() instanceof SchemaPath)) {
        continue;
      }
      NameSegment expr = ((SchemaPath) ex.getExpr()).getRootSegment();
      NameSegment ref = ex.getRef().getRootSegment();
      boolean refHasPrefix = ref.getPath()
          .contains(StarColumnHelper.PREFIX_DELIMITER);
      boolean exprContainsStar = expr.getPath()
          .contains(SchemaPath.DYNAMIC_STAR);

      if (refHasPrefix || exprContainsStar) {
        needed = true;
        break;
      }
    }
    return needed;
  }

  private void setupExpression(NamedExpression namedExpression)
      throws SchemaChangeException {
    result.clear();
    if (classify && namedExpression.getExpr() instanceof SchemaPath) {
      classifyExpr(namedExpression, incomingBatch, result);

      if (result.isStar) {
        setupImplicitColumnRef(namedExpression);
        return;
      }
    } else {
      // For the columns which do not needed to be classified,
      // it is still necessary to ensure the output column name is unique
      result.outputNames = Lists.newArrayList();
      String outputName = getRef(namedExpression).getRootSegment().getPath(); // moved
                                                                              // to
                                                                              // before
                                                                              // the
                                                                              // if
      addToResultMaps(outputName, result, true);
    }
    String outputName = getRef(namedExpression).getRootSegment().getPath();
    if (result != null && result.outputNames != null
        && result.outputNames.size() > 0) {
      boolean isMatched = false;
      for (int j = 0; j < result.outputNames.size(); j++) {
        if (!result.outputNames.get(j).isEmpty()) {
          outputName = result.outputNames.get(j);
          isMatched = true;
          break;
        }
      }

      if (!isMatched) {
        return;
      }
    }

    LogicalExpression expr = ExpressionTreeMaterializer.materialize(
        namedExpression.getExpr(), incomingBatch, collector,
        functionLookupContext, true, unionTypeEnabled);
    if (collector.hasErrors()) {
      throw new SchemaChangeException(String.format(
          "Failure while trying to materialize incoming schema.  Errors:\n %s.",
          collector.toErrorString()));
    }

    // Add value vector to transfer if direct reference and this is allowed,
    // otherwise, add to evaluation stack.
    if (expr instanceof ValueVectorReadExpression
        && incomingBatch.getSchema()
            .getSelectionVectorMode() == SelectionVectorMode.NONE
        && !((ValueVectorReadExpression) expr).hasReadPath() && !isAnyWildcard
        && !transferFieldIds.contains(
            ((ValueVectorReadExpression) expr).getFieldId().getFieldIds()[0])) {
      setupDirectTransfer(namedExpression, expr);
    } else if (expr instanceof DrillFuncHolderExpr
        && ((DrillFuncHolderExpr) expr).getHolder()
            .isComplexWriterFuncHolder()) {
      setupFnCall(namedExpression, expr);
    } else {
      // need to do evaluation.
      setupExprEval(namedExpression, expr, outputName);
    }
  }

  private void setupImplicitColumnRef(NamedExpression namedExpression)
      throws SchemaChangeException {
    // The value indicates which wildcard we are processing now
    Integer value = result.prefixMap.get(result.prefix);
    if (value != null && value == 1) {
      int k = 0;
      for (VectorWrapper<?> wrapper : incomingBatch) {
        ValueVector vvIn = wrapper.getValueVector();
        if (k > result.outputNames.size() - 1) {
          assert false;
        }
        String name = result.outputNames.get(k++); // get the renamed column
                                                   // names
        if (name.isEmpty()) {
          continue;
        }

        if (isImplicitFileColumn(vvIn)) {
          continue;
        }

        vectorState.addTransferField(name, vvIn);
      }
    } else if (value != null && value > 1) { // subsequent wildcards should do a
                                             // copy of incoming value vectors
      int k = 0;
      for (VectorWrapper<?> wrapper : incomingBatch) {
        ValueVector vvIn = wrapper.getValueVector();
        SchemaPath originalPath = SchemaPath
            .getSimplePath(vvIn.getField().getName());
        if (k > result.outputNames.size() - 1) {
          assert false;
        }
        String name = result.outputNames.get(k++); // get the renamed column
                                                   // names
        if (name.isEmpty()) {
          continue;
        }

        if (isImplicitFileColumn(vvIn)) {
          continue;
        }

        LogicalExpression expr = ExpressionTreeMaterializer.materialize(
            originalPath, incomingBatch, collector, functionLookupContext);
        if (collector.hasErrors()) {
          throw new SchemaChangeException(String.format(
              "Failure while trying to materialize incomingBatch schema.  Errors:\n %s.",
              collector.toErrorString()));
        }

        ValueVectorWriteExpression write = vectorState.addOutputVector(name,
            expr);
        cg.addExpr(write, ClassGenerator.BlkCreateMode.TRUE_IF_BOUND);
      }
    }
  }

  private void setupDirectTransfer(NamedExpression namedExpression,
      LogicalExpression expr) {
    FieldReference ref = getRef(namedExpression);
    int fid = vectorState.addDirectTransfer(ref,
        (ValueVectorReadExpression) expr, incomingBatch);
    transferFieldIds.add(fid);
  }

  private void setupFnCall(NamedExpression namedExpression,
      LogicalExpression expr) {
    // Need to process ComplexWriter function evaluation.
    vectorState.initComplexWriters();

    // The reference name will be passed to ComplexWriter, used as the name of
    // the output vector from the writer.
    ((DrillFuncHolderExpr) expr).setFieldReference(namedExpression.getRef());
    cg.addExpr(expr, ClassGenerator.BlkCreateMode.TRUE_IF_BOUND);
    vectorState.addComplexField(namedExpression.getRef());
  }

  private void setupExprEval(NamedExpression namedExpression,
      LogicalExpression expr, String outputName) {
    ValueVectorWriteExpression write = vectorState.addEvalVector(outputName,
        expr, incomingBatch);
    cg.addExpr(write, ClassGenerator.BlkCreateMode.TRUE_IF_BOUND);
  }

  private boolean isImplicitFileColumn(ValueVector vvIn) {
    return columnExplorer
        .isImplicitOrInternalFileColumn(vvIn.getField().getName());
  }

  private void classifyExpr(NamedExpression ex, RecordBatch incoming,
      ProjectionMaterializer.ClassifierResult result) {
    NameSegment expr = ((SchemaPath) ex.getExpr()).getRootSegment();
    NameSegment ref = ex.getRef().getRootSegment();
    boolean exprHasPrefix = expr.getPath()
        .contains(StarColumnHelper.PREFIX_DELIMITER);
    boolean refHasPrefix = ref.getPath()
        .contains(StarColumnHelper.PREFIX_DELIMITER);
    boolean exprIsStar = expr.getPath().equals(SchemaPath.DYNAMIC_STAR);
    boolean refContainsStar = ref.getPath().contains(SchemaPath.DYNAMIC_STAR);
    boolean exprContainsStar = expr.getPath().contains(SchemaPath.DYNAMIC_STAR);
    boolean refEndsWithStar = ref.getPath().endsWith(SchemaPath.DYNAMIC_STAR);

    String exprPrefix = EMPTY_STRING;
    String exprSuffix = expr.getPath();

    if (exprHasPrefix) {
      // get the prefix of the expr
      String[] exprComponents = expr.getPath()
          .split(StarColumnHelper.PREFIX_DELIMITER, 2);
      assert (exprComponents.length == 2);
      exprPrefix = exprComponents[0];
      exprSuffix = exprComponents[1];
      result.prefix = exprPrefix;
    }

    boolean exprIsFirstWildcard = false;
    if (exprContainsStar) {
      result.isStar = true;
      Integer value = result.prefixMap.get(exprPrefix);
      if (value == null) {
        result.prefixMap.put(exprPrefix, 1);
        exprIsFirstWildcard = true;
      } else {
        result.prefixMap.put(exprPrefix, value + 1);
      }
    }

    int incomingSchemaSize = incoming.getSchema().getFieldCount();

    // input is '*' and output is 'prefix_*'
    if (exprIsStar && refHasPrefix && refEndsWithStar) {
      String[] components = ref.getPath()
          .split(StarColumnHelper.PREFIX_DELIMITER, 2);
      assert (components.length == 2);
      String prefix = components[0];
      result.outputNames = Lists.newArrayList();
      for (VectorWrapper<?> wrapper : incoming) {
        ValueVector vvIn = wrapper.getValueVector();
        String name = vvIn.getField().getName();

        // add the prefix to the incoming column name
        String newName = prefix + StarColumnHelper.PREFIX_DELIMITER + name;
        addToResultMaps(newName, result, false);
      }
    }
    // input and output are the same
    else if (expr.getPath().equalsIgnoreCase(ref.getPath())
        && (!exprContainsStar || exprIsFirstWildcard)) {
      if (exprContainsStar && exprHasPrefix) {
        assert exprPrefix != null;

        int k = 0;
        result.outputNames = Lists.newArrayListWithCapacity(incomingSchemaSize);
        for (int j = 0; j < incomingSchemaSize; j++) {
          result.outputNames.add(EMPTY_STRING); // initialize
        }

        for (VectorWrapper<?> wrapper : incoming) {
          ValueVector vvIn = wrapper.getValueVector();
          String incomingName = vvIn.getField().getName();
          // get the prefix of the name
          String[] nameComponents = incomingName
              .split(StarColumnHelper.PREFIX_DELIMITER, 2);
          // if incoming value vector does not have a prefix, ignore it since
          // this expression is not referencing it
          if (nameComponents.length <= 1) {
            k++;
            continue;
          }
          String namePrefix = nameComponents[0];
          if (exprPrefix.equalsIgnoreCase(namePrefix)) {
            if (!result.outputMap.containsKey(incomingName)) {
              result.outputNames.set(k, incomingName);
              result.outputMap.put(incomingName, incomingName);
            }
          }
          k++;
        }
      } else {
        result.outputNames = Lists.newArrayList();
        if (exprContainsStar) {
          for (VectorWrapper<?> wrapper : incoming) {
            ValueVector vvIn = wrapper.getValueVector();
            String incomingName = vvIn.getField().getName();
            if (refContainsStar) {
              addToResultMaps(incomingName, result, true); // allow dups since
                                                           // this is likely
                                                           // top-level project
            } else {
              addToResultMaps(incomingName, result, false);
            }
          }
        } else {
          String newName = expr.getPath();
          if (!refHasPrefix && !exprHasPrefix) {
            addToResultMaps(newName, result, true); // allow dups since this is
                                                    // likely top-level project
          } else {
            addToResultMaps(newName, result, false);
          }
        }
      }
    }

    // Input is wildcard and it is not the first wildcard
    else if (exprIsStar) {
      result.outputNames = Lists.newArrayList();
      for (VectorWrapper<?> wrapper : incoming) {
        ValueVector vvIn = wrapper.getValueVector();
        String incomingName = vvIn.getField().getName();
        addToResultMaps(incomingName, result, true); // allow dups since this is
                                                     // likely top-level project
      }
    }

    // Only the output has prefix
    else if (!exprHasPrefix && refHasPrefix) {
      result.outputNames = Lists.newArrayList();
      String newName = ref.getPath();
      addToResultMaps(newName, result, false);
    }
    // Input has prefix but output does not
    else if (exprHasPrefix && !refHasPrefix) {
      int k = 0;
      result.outputNames = Lists.newArrayListWithCapacity(incomingSchemaSize);
      for (int j = 0; j < incomingSchemaSize; j++) {
        result.outputNames.add(EMPTY_STRING); // initialize
      }

      for (VectorWrapper<?> wrapper : incoming) {
        ValueVector vvIn = wrapper.getValueVector();
        String name = vvIn.getField().getName();
        String[] components = name.split(StarColumnHelper.PREFIX_DELIMITER, 2);
        if (components.length <= 1) {
          k++;
          continue;
        }
        String namePrefix = components[0];
        String nameSuffix = components[1];
        if (exprPrefix.equalsIgnoreCase(namePrefix)) { // // case insensitive
                                                       // matching of prefix.
          if (refContainsStar) {
            // remove the prefix from the incoming column names
            String newName = getUniqueName(nameSuffix, result); // for top level
                                                                // we need to
                                                                // make names
                                                                // unique
            result.outputNames.set(k, newName);
          } else if (exprSuffix.equalsIgnoreCase(nameSuffix)) { // case
                                                                // insensitive
                                                                // matching of
                                                                // field name.
            // example: ref: $f1, expr: T0<PREFIX><column_name>
            String newName = ref.getPath();
            result.outputNames.set(k, newName);
          }
        } else {
          result.outputNames.add(EMPTY_STRING);
        }
        k++;
      }
    }
    // input and output have prefixes although they could be different...
    else if (exprHasPrefix && refHasPrefix) {
      String[] input = expr.getPath().split(StarColumnHelper.PREFIX_DELIMITER,
          2);
      assert (input.length == 2);
      assert false : "Unexpected project expression or reference"; // not
                                                                   // handled
                                                                   // yet
    } else {
      // if the incoming schema's column name matches the expression name of the
      // Project,
      // then we just want to pick the ref name as the output column name

      result.outputNames = Lists.newArrayList();
      for (VectorWrapper<?> wrapper : incoming) {
        ValueVector vvIn = wrapper.getValueVector();
        String incomingName = vvIn.getField().getName();
        if (expr.getPath().equalsIgnoreCase(incomingName)) { // case insensitive
                                                             // matching of
                                                             // field name.
          String newName = ref.getPath();
          addToResultMaps(newName, result, true);
        }
      }
    }
  }

  private String getUniqueName(String name,
      ProjectionMaterializer.ClassifierResult result) {
    Integer currentSeq = (Integer) result.sequenceMap.get(name);
    if (currentSeq == null) { // name is unique, so return the original name
      result.sequenceMap.put(name, -1);
      return name;
    }
    // create a new name
    int newSeq = currentSeq + 1;
    String newName = name + newSeq;
    result.sequenceMap.put(name, newSeq);
    result.sequenceMap.put(newName, -1);

    return newName;
  }

  /**
   * Helper method to ensure unique output column names. If allowDupsWithRename
   * is set to true, the original name will be appended with a suffix number to
   * ensure uniqueness. Otherwise, the original column would not be renamed even
   * even if it has been used
   *
   * @param origName
   *          the original input name of the column
   * @param result
   *          the data structure to keep track of the used names and decide what
   *          output name should be to ensure uniqueness
   * @param allowDupsWithRename
   *          if the original name has been used, is renaming allowed to ensure
   *          output name unique
   */
  private void addToResultMaps(String origName,
      ProjectionMaterializer.ClassifierResult result,
      boolean allowDupsWithRename) {
    String name = origName;
    if (allowDupsWithRename) {
      name = getUniqueName(origName, result);
    }
    if (!result.outputMap.containsKey(name)) {
      result.outputNames.add(name);
      result.outputMap.put(name, name);
    } else {
      result.outputNames.add(EMPTY_STRING);
    }
  }

  // Hack to make ref and full work together... need to figure out if this is
  // still necessary.
  private FieldReference getRef(NamedExpression e) {
    return e.getRef();
  }
}
