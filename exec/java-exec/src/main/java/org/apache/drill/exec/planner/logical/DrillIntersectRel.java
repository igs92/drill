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
package org.apache.drill.exec.planner.logical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.drill.common.logical.data.Intersect.Builder;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.exec.planner.common.DrillIntersectRelBase;

import java.util.List;
import java.util.stream.IntStream;

public class DrillIntersectRel extends DrillIntersectRelBase implements DrillRel {

  public DrillIntersectRel(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, boolean all) {
    super(cluster, traits, inputs, all);
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
    Builder builder = org.apache.drill.common.logical.data.Intersect.builder();
    IntStream.range(0, inputs.size())
        .mapToObj(ord -> implementor.visitChild(this, ord, inputs.get(ord)))
        .forEach(builder::addInput);
    return builder.all(all).build();
  }

  @Override
  public DrillIntersectRel copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
    return new DrillIntersectRel(getCluster(), traitSet, inputs, all);
  }
}
