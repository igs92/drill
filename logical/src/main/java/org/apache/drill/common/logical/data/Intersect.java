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
package org.apache.drill.common.logical.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.logical.data.visitors.LogicalVisitor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@JsonTypeName("intersect")
public class Intersect extends LogicalOperatorBase {

  private final boolean all;
  private final List<LogicalOperator> inputs;

  @JsonCreator
  public Intersect(@JsonProperty("all") boolean all, @JsonProperty("inputs") List<LogicalOperator> inputs) {
    this.all = all;
    this.inputs = inputs;
  }

  public boolean isAll() {
    return all;
  }

  public List<LogicalOperator> getInputs() {
    return inputs;
  }

  @Override
  public <T, X, E extends Throwable> T accept(LogicalVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitIntersect(this, value);
  }

  @Override
  public Iterator<LogicalOperator> iterator() {
    return inputs.iterator();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder extends AbstractBuilder<Intersect> {

    private final List<LogicalOperator> inputs = new ArrayList<>();
    private boolean all;

    public Builder addInput(LogicalOperator input) {
      inputs.add(input);
      return this;
    }

    public Builder all(boolean all) {
      this.all = all;
      return this;
    }

    @Override
    public Intersect build() {
      return new Intersect(all, inputs);
    }
  }
}
