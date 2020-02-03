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
package org.apache.drill.exec.physical.base;

import org.apache.drill.exec.physical.config.BroadcastSender;
import org.apache.drill.exec.physical.config.Filter;
import org.apache.drill.exec.physical.config.FlattenPOP;
import org.apache.drill.exec.physical.config.HashAggregate;
import org.apache.drill.exec.physical.config.HashPartitionSender;
import org.apache.drill.exec.physical.config.HashToRandomExchange;
import org.apache.drill.exec.physical.config.Intersect;
import org.apache.drill.exec.physical.config.IteratorValidator;
import org.apache.drill.exec.physical.config.LateralJoinPOP;
import org.apache.drill.exec.physical.config.Limit;
import org.apache.drill.exec.physical.config.MergingReceiverPOP;
import org.apache.drill.exec.physical.config.OrderedPartitionSender;
import org.apache.drill.exec.physical.config.ProducerConsumer;
import org.apache.drill.exec.physical.config.Project;
import org.apache.drill.exec.physical.config.RangePartitionSender;
import org.apache.drill.exec.physical.config.RowKeyJoinPOP;
import org.apache.drill.exec.physical.config.Screen;
import org.apache.drill.exec.physical.config.SingleSender;
import org.apache.drill.exec.physical.config.Sort;
import org.apache.drill.exec.physical.config.StatisticsAggregate;
import org.apache.drill.exec.physical.config.StatisticsMerge;
import org.apache.drill.exec.physical.config.StreamingAggregate;
import org.apache.drill.exec.physical.config.Trace;
import org.apache.drill.exec.physical.config.UnionAll;
import org.apache.drill.exec.physical.config.UnnestPOP;
import org.apache.drill.exec.physical.config.UnorderedReceiver;
import org.apache.drill.exec.physical.config.UnpivotMaps;
import org.apache.drill.exec.physical.config.Values;
import org.apache.drill.exec.physical.config.WindowPOP;

/**
 * Visitor class designed to traversal of a operator tree.  Basis for a number of operator manipulations including fragmentation and materialization.
 * @param <R> The class associated with the return of each visit method.
 * @param <V> The class object associated with additional data required for a particular operator modification.
 * @param <T> An optional exception class that can be thrown when a portion of a modification or traversal fails.  Must extend Throwable.  In the case where the visitor does not throw any caught exception, this can be set as RuntimeException.
 */
public interface PhysicalVisitor<R, V, T extends Throwable> {
  org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PhysicalVisitor.class);


  R visitExchange(Exchange exchange, V value) throws T;
  R visitGroupScan(GroupScan groupScan, V value) throws T;
  R visitSubScan(SubScan subScan, V value) throws T;
  R visitStore(Store store, V value) throws T;
  R visitFilter(Filter filter, V value) throws T;
  R visitUnion(UnionAll union, V value) throws T;
  R visitProject(Project project, V value) throws T;
  R visitTrace(Trace trace, V value) throws T;
  R visitSort(Sort sort, V value) throws T;
  R visitLimit(Limit limit, V value) throws T;
  R visitFlatten(FlattenPOP flatten, V value) throws T;
  R visitSender(Sender sender, V value) throws T;
  R visitRowKeyJoin(RowKeyJoinPOP join, V value) throws T;
  R visitReceiver(Receiver receiver, V value) throws T;
  R visitStreamingAggregate(StreamingAggregate agg, V value) throws T;
  R visitStatisticsAggregate(StatisticsAggregate agg, V value) throws T;
  R visitStatisticsMerge(StatisticsMerge agg, V value) throws T;
  R visitHashAggregate(HashAggregate agg, V value) throws T;
  R visitWriter(Writer op, V value) throws T;
  R visitUnpivot(UnpivotMaps op, V value) throws T;
  R visitValues(Values op, V value) throws T;
  R visitOp(PhysicalOperator op, V value) throws T;

  R visitHashPartitionSender(HashPartitionSender op, V value) throws T;
  R visitOrderedPartitionSender(OrderedPartitionSender op, V value) throws T;
  R visitUnorderedReceiver(UnorderedReceiver op, V value) throws T;
  R visitMergingReceiver(MergingReceiverPOP op, V value) throws T;
  R visitHashPartitionSender(HashToRandomExchange op, V value) throws T;
  R visitRangePartitionSender(RangePartitionSender op, V value) throws T;
  R visitBroadcastSender(BroadcastSender op, V value) throws T;
  R visitScreen(Screen op, V value) throws T;
  R visitSingleSender(SingleSender op, V value) throws T;
  R visitWindowFrame(WindowPOP op, V value) throws T;
  R visitProducerConsumer(ProducerConsumer op, V value) throws T;
  R visitUnnest(UnnestPOP unnest, V value) throws T;
  R visitLateralJoin(LateralJoinPOP lateralJoinPOP, V value) throws T;

  R visitIteratorValidator(IteratorValidator op, V value) throws T;

  R visitIntersect(Intersect op, V value) throws T;
}
