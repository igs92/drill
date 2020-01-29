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
package org.apache.drill.common.logical.data.visitors;


import org.apache.drill.common.logical.data.LateralJoin;
import org.apache.drill.common.logical.data.Unnest;
import org.apache.drill.common.logical.data.Analyze;
import org.apache.drill.common.logical.data.Values;
import org.apache.drill.common.logical.data.Filter;
import org.apache.drill.common.logical.data.Flatten;
import org.apache.drill.common.logical.data.GroupingAggregate;
import org.apache.drill.common.logical.data.Join;
import org.apache.drill.common.logical.data.Limit;
import org.apache.drill.common.logical.data.Order;
import org.apache.drill.common.logical.data.Project;
import org.apache.drill.common.logical.data.RunningAggregate;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.common.logical.data.Store;
import org.apache.drill.common.logical.data.Transform;
import org.apache.drill.common.logical.data.Union;
import org.apache.drill.common.logical.data.Window;
import org.apache.drill.common.logical.data.Writer;

/**
 * Visitor class designed to traversal of a operator tree.  Basis for a number of operator manipulations including fragmentation and materialization.
 * @param <R> The class associated with the return of each visit method.
 * @param <V> The class object associated with additional data required for a particular operator modification.
 * @param <T> An optional exception class that can be thrown when a portion of a modification or traversal fails.  Must extend Throwable.  In the case where the visitor does not throw any caught exception, this can be set as RuntimeException.
 */
public interface LogicalVisitor<R, V, T extends Throwable> {
    org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LogicalVisitor.class);

    R visitScan(Scan scan, V value) throws T;
    R visitStore(Store store, V value) throws T;
    R visitGroupingAggregate(GroupingAggregate groupBy, V value) throws T;
    R visitFilter(Filter filter, V value) throws T;
    R visitFlatten(Flatten flatten, V value) throws T;
    R visitAnalyze(Analyze analyze, V value) throws T;
    R visitProject(Project project, V value) throws T;
    R visitValues(Values constant, V value) throws T;
    R visitOrder(Order order, V value) throws T;
    R visitJoin(Join join, V value) throws T;
    R visitLimit(Limit limit, V value) throws T;
    R visitRunningAggregate(RunningAggregate runningAggregate, V value) throws T;
    R visitTransform(Transform transform, V value) throws T;
    R visitUnion(Union union, V value) throws T;
    R visitWindow(Window window, V value) throws T;
    R visitWriter(Writer writer, V value) throws T;
    R visitUnnest(Unnest unnest, V value) throws T;
    R visitLateralJoin(LateralJoin lateralJoin, V value) throws T;
}
