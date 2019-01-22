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
package org.apache.drill.exec.store.hive.readers.inspectors;

import org.apache.hadoop.mapred.RecordReader;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * To implement skip footer logic this records inspector will buffer N number of incoming read records in queue
 * and make sure they are skipped when input is fully processed. FIFO method of queuing is used for these purposes.
 */
/* todo: simplify this class
    1: I believe the AbstractRecordsInspector is redundant, none of it's non-abstract methods or fields is used here,
       the abstract class may be replaced by interface or even removed in future
    2: I believe internals of the class may be replaced only by array for valueHolders and few additional
       int fields.

    How the class works ? Consider reading example table with one column, we don't know
    how much records are stored in table but just know how much records to skip in the end.
    So this class is just used to defer records processing and it's internals is tightly coupled to
    it's usage. For example consider reading table with 2 footer records
    (for simplicity I used just 5 records, but actually we don't know amount of records
    in table while reading it):

     table_column
    --------------
      a
      b
      c
     footer_1
     footer_2
    ---------------

   0) Init state of the class :
       footerCount:         2 (final)
       footerBuffer:        empty queue: []
       valueHolders:        list with 3 holder objects (lets mark holder as {} and list as []): [{},{},{}]
       readRecordsCount:    0
    ===================================
    Here is how the SkipFooterRecordsInspector inspector is used while reading records,
    to understand what's going on with it we need to learn internal state of inspector after each iteration.

      while (!inspector.isBatchFull() &&
             hasNextValue(inspector.getValueHolder()) // read record into free holder
      ) {
          Object value = inspector.getNextValue(); // add record to fifo queue and
                                                   // return null(if queue size not greater than footerCount) or first value of queue
          if (value != null) {
           // handle and save record to value vectors
          }
      }

    1) First iteration (reading 'a'):
       inside loop:
           inspector.getNextValue() -> null
       state after loop:
           footerBuffer:[{a}]
           valueHolders: [{a},{},{}]
           readRecordsCount: 1
    2) Second iteration (reading 'b'):
       inside loop:
           inspector.getNextValue() -> null
       state after loop:
           footerBuffer:[{a}, {b}]
           valueHolders: [{a},{b},{}]
           readRecordsCount: 2
    3) Third iteration (reading 'c'):
       inside loop:
           inspector.getNextValue() -> {a}
       state after loop:
           footerBuffer:[{b}, {c}]
           valueHolders: [{a},{b},{c}]
           readRecordsCount: 3
    4) Fourth iteration (reading 'footer_1'):
       inside loop:
           inspector.getNextValue() -> {b}
       state after loop:
           footerBuffer:[{c}, {footer_1}]
           valueHolders: [{footer_1},{b},{c}]
           readRecordsCount: 4
    5) Last fifth iteration  (reading 'footer_2'):
       inside loop:
           inspector.getNextValue() -> {c}
       state after loop:
           footerBuffer:[{footer_1},{footer_2}]
           valueHolders: [{footer_1},{footer_2},{c}]
           readRecordsCount: 5
    6) Reading cycle ended and we can conclude that first footerCount (two) iterations
       we returned null from getNextValue() but saved read records into queue, and then started
       to return them one by one. But after last iteration queue contains only footer records,
       which will never be processed. That's is the main purpose of this class: ensure that in
       record reading loop  inspector.getNextValue() will never return footer records.
*/
public class SkipFooterRecordsInspector extends AbstractRecordsInspector {

  private final int footerCount;
  private Queue<Object> footerBuffer;
  private final List<Object> valueHolders;
  private long readRecordsCount;

  public SkipFooterRecordsInspector(RecordReader<Object, Object> reader, int footerCount) {
    this.footerCount = footerCount;
    this.footerBuffer = new LinkedList<>();
    this.valueHolders = initializeValueHolders(reader, footerCount);
  }

  /**
   * Returns next available value holder where value should be written from the cached value holders.
   * Current available holder is determined by getting mod for actually read records.
   *
   * @return value holder
   */
  @Override
  public Object getValueHolder() {
    int availableHolderIndex = (int) readRecordsCount % valueHolders.size();
    return valueHolders.get(availableHolderIndex);
  }

  /**
   * Buffers current value holder with written value
   * and returns last buffered value if number of buffered values exceeds N records to skip.
   *
   * @return next available value holder with written value, null otherwise
   */
  @Override
  public Object getNextValue() {
    footerBuffer.add(getValueHolder());
    readRecordsCount++;
    if (footerBuffer.size() <= footerCount) {
      return null;
    }
    return footerBuffer.poll();
  }

  /**
   * Creates buffer of value holders, so these holders can be re-used.
   * Holders quantity depends on number of lines to skip in the end of the file plus one.
   *
   * @param reader record reader
   * @param footerCount number of lines to skip at the end of the file
   * @return list of value holders
   */
  private List<Object> initializeValueHolders(RecordReader<Object, Object> reader, int footerCount) {
    List<Object> valueHolder = new ArrayList<>(footerCount + 1);
    for (int i = 0; i <= footerCount; i++) {
      valueHolder.add(reader.createValue());
    }
    return valueHolder;
  }

}
