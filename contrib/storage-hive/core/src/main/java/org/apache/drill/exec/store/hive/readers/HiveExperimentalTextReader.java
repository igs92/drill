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
package org.apache.drill.exec.store.hive.readers;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.store.hive.HivePartition;
import org.apache.drill.exec.store.hive.HiveTableWithColumnCache;
import org.apache.drill.exec.store.hive.HiveUtilities;
import org.apache.drill.exec.store.hive.readers.inspectors.SkipFooterRecordsInspector;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.security.UserGroupInformation;

public class HiveExperimentalTextReader extends HiveExperimentalReader {

  private SkipFooterRecordsInspector skipFooterValueHolder;

  /**
   * The ordering of incoming parameters is very important because concrete mapredReader instances
   * are created using  readers initializers. todo: throw out reflection and replace with direct references to constructors
   *
   * @param table            metadata about Hive table being read
   * @param partition        holder of metadata about table partitioning
   * @param inputSplits      input splits for reading data from distributed storage
   * @param projectedColumns target columns for scan
   * @param context          fragmentContext of fragment
   * @param hiveConf         Hive configuration
   * @param proxyUgi         user/group info to be used for initialization
   */
  public HiveExperimentalTextReader(HiveTableWithColumnCache table, HivePartition partition, Collection<InputSplit> inputSplits, List<SchemaPath> projectedColumns, FragmentContext context, HiveConf hiveConf, UserGroupInformation proxyUgi) {
    super(table, partition, inputSplits, projectedColumns, context, hiveConf, proxyUgi);
  }

  @Override
  protected void internalInit(Properties hiveTableProperties) {
    int skipHeaderCount = HiveUtilities.retrieveIntProperty(hiveTableProperties, serdeConstants.HEADER_COUNT, -1);

    // skip first N records to apply skip header policy
    try {
      for (int i = 0; i < skipHeaderCount; i++) {
        if (!hasNextValue(valueHolder)) {
          // no more records to skip, we drained the table
          empty = true;
          break;
        }
      }
    } catch (IOException | ExecutionSetupException e) {
      e.printStackTrace(); // todo: fixme
    }

    // if table was drained while skipping first N records, there is no need to check for skip footer logic
    if (!empty) {
      int skipFooterCount = HiveUtilities.retrieveIntProperty(hiveTableProperties, serdeConstants.FOOTER_COUNT, -1);

      // if we need to skip N last records, use records skipFooterValueHolder which will buffer records while reading
      if (skipFooterCount > 0) {
        skipFooterValueHolder = new SkipFooterRecordsInspector(mapredReader, skipFooterCount);
      }
    }
  }

  //todo: need to simplify the method
  @Override
  public int next() {
    if (skipFooterValueHolder == null) {
      /* no need to apply skip footer, just use more parent method,
      this case mean that we applied only skip header logic to the table*/
      return super.next();
    } else {
      try {
        // starting new batch, reset processed records count
        skipFooterValueHolder.reset();

        // process records till batch is full or all records were processed
        while (!skipFooterValueHolder.isBatchFull() && hasNextValue(skipFooterValueHolder.getValueHolder())) {
          Object value = skipFooterValueHolder.getNextValue();
          if (value != null) { // todo: need to avoid the if later, null returned only few times for SkipFooterRecordsInspector and never for DefaultRecordsInspector
            Object deSerializedValue = partitionToTableSchemaConverter.convert(partitionDeserializer.deserialize((Writable) value));
            outputWriter.setPosition(skipFooterValueHolder.getProcessedRecordCount());
            readHiveRecordAndInsertIntoRecordBatch(deSerializedValue);
            skipFooterValueHolder.incrementProcessedRecordCount();
          }
        }
        outputWriter.setValueCount(skipFooterValueHolder.getProcessedRecordCount());

        return skipFooterValueHolder.getProcessedRecordCount();
      } catch (ExecutionSetupException | IOException | SerDeException e) {
        throw new DrillRuntimeException(e.getMessage(), e);
      }
    }
  }

}
