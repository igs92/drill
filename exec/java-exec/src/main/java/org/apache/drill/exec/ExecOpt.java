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
package org.apache.drill.exec;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.server.options.OptionSet;
import org.apache.drill.exec.server.options.OptionValidator;
import org.apache.drill.exec.server.options.TypeValidators;

import java.util.List;

import static org.apache.drill.exec.server.options.TypeValidators.acceptStrings;
import static org.apache.drill.exec.server.options.TypeValidators.booleanType;
import static org.apache.drill.exec.server.options.TypeValidators.doubleRange;
import static org.apache.drill.exec.server.options.TypeValidators.intType;
import static org.apache.drill.exec.server.options.TypeValidators.longRange;
import static org.apache.drill.exec.server.options.TypeValidators.positiveLong;


public enum ExecOpt {
  ZK_RETRY_TIMES("drill.exec.zk.retry.count"),
  ZK_RETRY_DELAY("drill.exec.zk.retry.delay"),
  ZK_CONNECTION("drill.exec.zk.connect"),
  ZK_TIMEOUT("drill.exec.zk.timeout"),
  ZK_ROOT("drill.exec.zk.root"),
  ZK_REFRESH("drill.exec.zk.refresh"),
  ZK_ACL_PROVIDER("drill.exec.zk.acl_provider"),
  ZK_APPLY_SECURE_ACL("drill.exec.zk.apply_secure_acl"),

  BIT_RETRY_TIMES("drill.exec.rpc.bit.server.retry.count"),
  BIT_RETRY_DELAY("drill.exec.rpc.bit.server.retry.delay"),
  BIT_PORT("drill.exec.rpc.bit.server.port"),
  BIT_DATA_PORT("drill.exec.rpc.bit.server.dataport"),
  BIT_RPC_TIMEOUT("drill.exec.rpc.bit.timeout", positiveLong()),

  USER_PORT("drill.exec.rpc.user.server.port"),
  USER_TIMEOUT("drill.exec.rpc.user.timeout"),

  CLIENT_RPC_THREADS("drill.exec.rpc.user.client.threads"),
  BIT_SERVER_RPC_THREADS("drill.exec.rpc.bit.server.threads"),
  USER_SERVER_RPC_THREADS("drill.exec.rpc.user.server.threads"),
  FRAG_RUNNER_RPC_TIMEOUT("drill.exec.rpc.fragrunner.timeout"),

  SERVICE_NAME("drill.exec.cluster-id"),

  TRACE_DUMP_DIR("drill.exec.trace.directory"),
  TRACE_DUMP_FS("drill.exec.trace.filesystem"),

  TMP_DIRS("drill.exec.tmp.directories"),
  TMP_FS("drill.exec.tmp.filesystem"),
  INCOMING_BUFFER_SIZE("drill.exec.buffer.size"),

  SPOOLING_BUFFER_DELETE("drill.exec.buffer.spooling.delete"),
  SPOOLING_BUFFER_MEMORY("drill.exec.buffer.spooling.size"),

  UNLIMITED_BUFFER_MAX_MEMORY_SIZE("drill.exec.buffer.unlimited_receiver.max_size"),
  BATCH_PURGE_THRESHOLD("drill.exec.sort.purge.threshold"),

  SPILL_FS("drill.exec.spill.fs"),
  SPILL_DIRS("drill.exec.spill.directories"),

  OUTPUT_BATCH_SIZE("drill.exec.memory.operator.output_batch_size", longRange(128, 512 * 1024 * 1024)
      .desc("Available as of Drill 1.13. " +
          "Limits the amount of memory that the Flatten, Merge Join, and External Sort operators allocate to outgoing batches.")),

  OUTPUT_BATCH_SIZE_AVAIL_MEM_FACTOR("drill.exec.memory.operator.output_batch_size_avail_mem_factor",
      doubleRange(0.01, 1.0)
          .desc("Based on the available system memory, adjusts the output batch size for buffered operators by the factor set.")),

  EXTERNAL_SORT_SPILL_DIRS("drill.exec.sort.external.spill.directories"),
  EXTERNAL_SORT_SPILL_FS("drill.exec.sort.external.spill.fs"),
  EXTERNAL_SORT_SPILL_FILE_SIZE("drill.exec.sort.external.spill.file_size"),
  EXTERNAL_SORT_MSORT_MAX_BATCHSIZE("drill.exec.sort.external.msort.batch.maxsize"),
  EXTERNAL_SORT_MERGE_LIMIT("drill.exec.sort.external.merge_limit"),
  EXTERNAL_SORT_SPILL_BATCH_SIZE("drill.exec.sort.external.spill.spill_batch_size"),
  EXTERNAL_SORT_MAX_MEMORY("drill.exec.sort.external.mem_limit"),
  EXTERNAL_SORT_BATCH_LIMIT("drill.exec.sort.external.batch_limit"),

  HASH_JOIN_HASHTABLE_CALC_TYPE("exec.hashjoin.hash_table_calc_type", acceptStrings("LEAN", "CONSERVATIVE")
      .desc("Sets the Hash Join Memory Calculator type. Default is LEAN. " +
          "This option also accepts CONSERVATIVE as a value.")),
  HASH_JOIN_SAFETY_FACTOR("exec.hashjoin.safety_factor", doubleRange(1.0, Double.MAX_VALUE)
      .desc("Sets the Hash Join Memory Calculation Safety; multiplies the internal size estimate. Default is 1.0")),
  HASH_JOIN_HASH_DOUBLE_FACTOR("exec.hashjoin.hash_double_factor", doubleRange(1.0, Double.MAX_VALUE)
      .desc("Sets the Hash Join Memory Calculation; doubling factor for the Hash-Table. Default is 2.0")),
  HASH_JOIN_FRAGMENTATION_FACTOR("exec.hashjoin.fragmentation_factor", doubleRange(1.0, Double.MAX_VALUE)
      .desc("Sets the Hash Join Memory Calculations; multiplies the internal estimates to account " +
          "for fragmentation. Default is 1.33")),
  HASH_JOIN_ROWS_IN_BATCH("exec.hashjoin.num_rows_in_batch", longRange(1, 65536)
      .desc("Sets the number of rows in the internal batches for Hash Join operations. Default is 1024")),
  HASH_JOIN_MAX_BATCHES_IN_MEMORY("exec.hashjoin.max_batches_in_memory", longRange(0, 65536)
      .desc("Sets the maximum number of batches allowed in memory before spilling is enforced " +
          "for Hash Join operations; used for testing purposes.")),
  HASH_JOIN_PARTITIONS_COUNT("exec.hashjoin.num_partitions", longRange(1, 128)
      .desc("Sets the initial number of internal partitions for Hash Join operations. " +
          "Default is 32. May reduce when memory is too small. Disables spilling if set to 1.")),
  HASH_JOIN_MAX_MEMORY("drill.exec.hashjoin.mem_limit", longRange(0, Long.MAX_VALUE)
      .desc("Enforces the maximum memory limit for the Hash Join operator (if non-zero);" +
          " used for testing purposes. Default is 0 (disabled).")),

  HASH_JOIN_SPILL_DIRS("drill.exec.hashjoin.spill.directories"),
  HASH_JOIN_SPILL_FS("drill.exec.hashjoin.spill.fs"),
  HASH_JOIN_FALLBACK_ENABLED("drill.exec.hashjoin.fallback.enabled", booleanType()
      .desc("Hash Joins ignore memory limits when this option is enabled (true). " +
          "When disabled (false), Hash Joins fail when memory is set too low.")),
  HASH_JOIN_ENABLE_RUNTIME_FILTER("exec.hashjoin.enable.runtime_filter", booleanType()),
  HASH_JOIN_BLOOM_FILTER_MAX_SIZE("exec.hashjoin.bloom_filter.max.size", intType()),
  HASH_JOIN_BLOOM_FILTER_FPP("exec.hashjoin.bloom_filter.fpp", doubleRange(Double.MIN_VALUE, 1.0)),
  HASH_JOIN_RUNTIME_FILTER_WAITING_ENABLE("exec.hashjoin.runtime_filter.waiting.enable", booleanType())


  /**/;

  public final String key;
  public final OptionValidator validator;

  ExecOpt(String key, TypeValidators.Builder validatorBuilder) {
    this.key = key;
    this.validator = validatorBuilder.key(key).build();
  }

  ExecOpt(String key) {
    this.key = key;
    this.validator = null;
  }

  public int intFrom(DrillConfig conf) {
    return conf.getInt(key);
  }

  public String stringFrom(DrillConfig conf) {
    return conf.getString(key);
  }

  public boolean notIn(DrillConfig config) {
    return !config.hasPath(key);
  }

  public boolean boolFrom(DrillConfig config) {
    return config.getBoolean(key);
  }

  public long longFrom(DrillConfig config) {
    return config.getLong(key);
  }

  public boolean isIn(DrillConfig config) {
    return config.hasPath(key);
  }

  public List<String> stringListFrom(DrillConfig config) {
    return config.getStringList(key);
  }

  public Long bytesFrom(DrillConfig config) {
    return config.getBytes(key);
  }

  public long longFrom(OptionSet optionSet) {
    return optionSet.getLong(key);
  }

  public int intFrom(OptionSet optionSet) {
    return (int) longFrom(optionSet);
  }

  public double doubleFrom(OptionSet optionSet) {
    return optionSet.getDouble(key);
  }

  public String stringFrom(OptionSet optionSet) {
    return optionSet.getString(key);
  }

  public boolean booleanFrom(OptionSet optionSet) {
    return optionSet.getBoolean(key);
  }
}
