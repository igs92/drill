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
package org.apache.drill.exec.server.options;

import org.apache.commons.collections.IteratorUtils;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.config.LogicalPlanPersistence;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ExecOpt;
import org.apache.drill.exec.compile.ClassCompilerSelector;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import org.apache.drill.exec.store.sys.store.provider.InMemoryStoreProvider;
import org.apache.drill.exec.util.AssertionUtil;
import org.apache.drill.shaded.guava.com.google.common.annotations.VisibleForTesting;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.drill.exec.server.options.OptionDefinition.def;
import static org.apache.drill.exec.server.options.OptionDefinition.defBuilder;
import static org.apache.drill.exec.server.options.OptionValue.AccessibleScopes.SESSION_AND_QUERY;
import static org.apache.drill.exec.server.options.OptionValue.AccessibleScopes.SYSTEM;
import static org.apache.drill.exec.server.options.OptionValue.AccessibleScopes.SYSTEM_AND_SESSION;

/**
 * <p>
 * {@link OptionManager} that holds options within
 * {@link org.apache.drill.exec.server.DrillbitContext}. Only one instance of
 * this class exists per drillbit. Options set at the system level affect the
 * entire system and persist between restarts.
 * </p>
 * <p>
 * All the system options are externalized into conf file. While adding a new
 * system option a validator should be added and the default value for the
 * option should be set in the conf files(example : drill-module.conf) under the
 * namespace drill.exec.options.
 * </p>
 * <p>
 * The SystemOptionManager loads all the validators and the default values for
 * the options are fetched from the config. The validators are populated with
 * the default values fetched from the config. If the option is not set in the
 * conf files config option is missing exception will be thrown.
 * </p>
 * <p>
 * If the option is set using ALTER, the value that is set will be returned.
 * Else the default value that is loaded into validator from the config will be
 * returned.
 * </p>
 */
public class SystemOptionManager extends BaseOptionManager implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(SystemOptionManager.class);

  /**
   * Creates the {@code OptionDefinitions} to be registered with the {@link SystemOptionManager}.
   *
   * @return A map
   */
  public static CaseInsensitiveMap<OptionDefinition> createDefaultOptionDefinitions() {
    // The deprecation says not to use the option in code. But, for backward
    // compatibility, we need to keep the old options in the table to avoid
    // failures if users reference the options. So, ignore deprecation warnings
    // here.
    @SuppressWarnings("deprecation") final OptionDefinition[] definitions = new OptionDefinition[]{
        def(PlannerSettings.CONSTANT_FOLDING),
        def(PlannerSettings.EXCHANGE),
        def(PlannerSettings.HASHAGG),
        def(PlannerSettings.STREAMAGG),
        defBuilder(PlannerSettings.TOPN).internal().build(),
        def(PlannerSettings.HASHJOIN),
        def(PlannerSettings.SEMIJOIN),
        def(PlannerSettings.MERGEJOIN),
        def(PlannerSettings.NESTEDLOOPJOIN),
        def(PlannerSettings.MULTIPHASE),
        def(PlannerSettings.BROADCAST),
        def(PlannerSettings.BROADCAST_THRESHOLD),
        def(PlannerSettings.BROADCAST_FACTOR),
        def(PlannerSettings.NESTEDLOOPJOIN_FACTOR),
        def(PlannerSettings.NLJOIN_FOR_SCALAR),
        def(PlannerSettings.JOIN_ROW_COUNT_ESTIMATE_FACTOR),
        def(PlannerSettings.MUX_EXCHANGE),
        def(PlannerSettings.ORDERED_MUX_EXCHANGE),
        def(PlannerSettings.DEMUX_EXCHANGE),
        def(PlannerSettings.PRODUCER_CONSUMER),
        def(PlannerSettings.PRODUCER_CONSUMER_QUEUE_SIZE),
        def(PlannerSettings.HASH_SINGLE_KEY),
        def(PlannerSettings.IDENTIFIER_MAX_LENGTH),
        def(PlannerSettings.HASH_JOIN_SWAP),
        def(PlannerSettings.HASH_JOIN_SWAP_MARGIN_FACTOR),
        def(PlannerSettings.PARTITION_SENDER_THREADS_FACTOR),
        def(PlannerSettings.PARTITION_SENDER_MAX_THREADS),
        def(PlannerSettings.PARTITION_SENDER_SET_THREADS),
        def(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE),
        def(PlannerSettings.HEP_OPT),
        def(PlannerSettings.PLANNER_MEMORY_LIMIT),
        def(PlannerSettings.HEP_PARTITION_PRUNING),
        def(PlannerSettings.ROWKEYJOIN_CONVERSION),
        def(PlannerSettings.ROWKEYJOIN_CONVERSION_USING_HASHJOIN),
        def(PlannerSettings.ROWKEYJOIN_CONVERSION_SELECTIVITY_THRESHOLD),
        def(PlannerSettings.FILTER_MIN_SELECTIVITY_ESTIMATE_FACTOR),
        def(PlannerSettings.FILTER_MAX_SELECTIVITY_ESTIMATE_FACTOR),
        def(PlannerSettings.TYPE_INFERENCE),
        def(PlannerSettings.IN_SUBQUERY_THRESHOLD),
        def(PlannerSettings.UNIONALL_DISTRIBUTE),
        def(PlannerSettings.PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING),
        def(PlannerSettings.PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD),
        def(PlannerSettings.QUOTING_IDENTIFIERS),
        def(PlannerSettings.JOIN_OPTIMIZATION),
        def(PlannerSettings.ENABLE_UNNEST_LATERAL),
        def(PlannerSettings.FORCE_2PHASE_AGGR), // for testing
        def(PlannerSettings.STATISTICS_USE),
        def(PlannerSettings.STATISTICS_MULTICOL_NDV_ADJUST_FACTOR),
        def(ExecOpt.HASH_JOIN_PARTITIONS_COUNT),
        defBuilder(ExecOpt.HASH_JOIN_MAX_MEMORY).scopes(SYSTEM).adminOnly().internal().build(),
        def(ExecOpt.HASH_JOIN_ROWS_IN_BATCH.validator),
        defBuilder(ExecOpt.HASH_JOIN_MAX_BATCHES_IN_MEMORY).scopes(SYSTEM).internal().build(),
        def(ExecOpt.HASH_JOIN_FALLBACK_ENABLED.validator),
        def(ExecOpt.HASH_JOIN_ENABLE_RUNTIME_FILTER.validator),
        def(ExecOpt.HASH_JOIN_BLOOM_FILTER_MAX_SIZE.validator),
        def(ExecOpt.HASH_JOIN_BLOOM_FILTER_FPP.validator),
        def(ExecOpt.HASH_JOIN_RUNTIME_FILTER_MAX_WAITING_TIME.validator),
        def(ExecOpt.HASH_JOIN_RUNTIME_FILTER_WAITING_ENABLE.validator),
        // ------------------------------------------- Index planning related options BEGIN --------------------------------------------------------------
        def(PlannerSettings.USE_SIMPLE_OPTIMIZER),
        def(PlannerSettings.INDEX_PLANNING),
        def(PlannerSettings.ENABLE_STATS),
        def(PlannerSettings.DISABLE_FULL_TABLE_SCAN),
        def(PlannerSettings.INDEX_MAX_CHOSEN_INDEXES_PER_TABLE),
        def(PlannerSettings.INDEX_FORCE_SORT_NONCOVERING),
        def(PlannerSettings.INDEX_USE_HASHJOIN_NONCOVERING),
        def(PlannerSettings.INDEX_COVERING_SELECTIVITY_THRESHOLD),
        def(PlannerSettings.INDEX_NONCOVERING_SELECTIVITY_THRESHOLD),
        def(PlannerSettings.INDEX_ROWKEYJOIN_COST_FACTOR),
        def(PlannerSettings.INDEX_STATS_ROWCOUNT_SCALING_FACTOR),
        // TODO: Deprecate the following 2 (also in PlannerSettings.java)
        def(PlannerSettings.INDEX_PREFER_INTERSECT_PLANS),
        def(PlannerSettings.INDEX_MAX_INDEXES_TO_INTERSECT),
        // ------------------------------------------- Index planning related options END   --------------------------------------------------------------
        def(ExecOpt.HASH_AGG_NUM_PARTITIONS),
        def(ExecConstants.HASHAGG_MAX_MEMORY_VALIDATOR),
        def(ExecConstants.HASHAGG_MIN_BATCHES_PER_PARTITION_VALIDATOR), // for tuning
        def(ExecConstants.HASHAGG_USE_MEMORY_PREDICTION_VALIDATOR), // for testing
        def(ExecConstants.HASHAGG_FALLBACK_ENABLED_VALIDATOR), // for enable/disable unbounded HashAgg
        def(ExecConstants.CAST_EMPTY_STRING_TO_NULL_OPTION),
        def(ExecConstants.OUTPUT_FORMAT_VALIDATOR),
        def(ExecConstants.PARQUET_BLOCK_SIZE_VALIDATOR),
        def(ExecConstants.PARQUET_WRITER_USE_SINGLE_FS_BLOCK_VALIDATOR),
        def(ExecConstants.PARQUET_PAGE_SIZE_VALIDATOR),
        def(ExecConstants.PARQUET_DICT_PAGE_SIZE_VALIDATOR),
        def(ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE_VALIDATOR),
        def(ExecConstants.PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING_VALIDATOR),
        def(ExecConstants.PARQUET_WRITER_USE_PRIMITIVE_TYPES_FOR_DECIMALS_VALIDATOR),
        def(ExecConstants.PARQUET_WRITER_LOGICAL_TYPE_FOR_DECIMALS_VALIDATOR),
        def(ExecConstants.PARQUET_VECTOR_FILL_THRESHOLD_VALIDATOR),
        def(ExecConstants.PARQUET_VECTOR_FILL_CHECK_THRESHOLD_VALIDATOR),
        def(ExecConstants.PARQUET_RECORD_READER_IMPLEMENTATION_VALIDATOR),
        def(ExecConstants.PARQUET_PAGEREADER_ASYNC_VALIDATOR),
        def(ExecConstants.PARQUET_PAGEREADER_QUEUE_SIZE_VALIDATOR),
        def(ExecConstants.PARQUET_PAGEREADER_ENFORCETOTALSIZE_VALIDATOR),
        def(ExecConstants.PARQUET_COLUMNREADER_ASYNC_VALIDATOR),
        def(ExecConstants.PARQUET_PAGEREADER_USE_BUFFERED_READ_VALIDATOR),
        def(ExecConstants.PARQUET_PAGEREADER_BUFFER_SIZE_VALIDATOR),
        def(ExecConstants.PARQUET_PAGEREADER_USE_FADVISE_VALIDATOR),
        def(ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP_VALIDATOR),
        def(ExecConstants.PARQUET_READER_STRINGS_SIGNED_MIN_MAX_VALIDATOR),
        def(ExecConstants.PARQUET_FLAT_READER_BULK_VALIDATOR),
        defBuilder(ExecConstants.PARQUET_FLAT_BATCH_NUM_RECORDS_VALIDATOR)
            .scopes(SYSTEM_AND_SESSION).adminOnly().internal().build(),
        defBuilder(ExecConstants.PARQUET_FLAT_BATCH_MEMORY_SIZE_VALIDATOR)
            .scopes(SYSTEM_AND_SESSION)
            .adminOnly()
            .internal()
            .build(),
        defBuilder(ExecConstants.PARQUET_COMPLEX_BATCH_NUM_RECORDS_VALIDATOR)
            .scopes(SYSTEM_AND_SESSION).adminOnly().internal().build(),
        def(ExecConstants.PARTITIONER_MEMORY_REDUCTION_THRESHOLD_VALIDATOR),
        def(ExecConstants.JSON_READER_ALL_TEXT_MODE_VALIDATOR),
        def(ExecConstants.JSON_WRITER_NAN_INF_NUMBERS_VALIDATOR),
        def(ExecConstants.JSON_READER_NAN_INF_NUMBERS_VALIDATOR),
        def(ExecConstants.JSON_READER_ESCAPE_ANY_CHAR_VALIDATOR),
        def(ExecConstants.STORE_TABLE_USE_SCHEMA_FILE_VALIDATOR),
        def(ExecConstants.ENABLE_UNION_TYPE),
        def(ExecConstants.TEXT_ESTIMATED_ROW_SIZE),
        def(ExecConstants.TEXT_WRITER_ADD_HEADER_VALIDATOR),
        def(ExecConstants.TEXT_WRITER_FORCE_QUOTES_VALIDATOR),
        def(ExecConstants.JSON_EXTENDED_TYPES),
        def(ExecConstants.JSON_WRITER_UGLIFY),
        def(ExecConstants.JSON_WRITER_SKIPNULLFIELDS),
        def(ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE_VALIDATOR),
        def(ExecConstants.JSON_SKIP_MALFORMED_RECORDS_VALIDATOR),
        def(ExecConstants.JSON_READER_PRINT_INVALID_RECORDS_LINE_NOS_FLAG_VALIDATOR),
        def(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL_VALIDATOR),
        def(ExecConstants.MONGO_READER_ALL_TEXT_MODE_VALIDATOR),
        def(ExecConstants.MONGO_READER_READ_NUMBERS_AS_DOUBLE_VALIDATOR),
        def(ExecConstants.MONGO_BSON_RECORD_READER_VALIDATOR),
        def(ExecConstants.KAFKA_READER_ALL_TEXT_MODE_VALIDATOR),
        def(ExecConstants.KAFKA_RECORD_READER_VALIDATOR),
        def(ExecConstants.KAFKA_POLL_TIMEOUT_VALIDATOR),
        def(ExecConstants.KAFKA_READER_READ_NUMBERS_AS_DOUBLE_VALIDATOR),
        def(ExecConstants.KAFKA_SKIP_MALFORMED_RECORDS_VALIDATOR),
        def(ExecConstants.KAFKA_READER_NAN_INF_NUMBERS_VALIDATOR),
        def(ExecConstants.KAFKA_READER_ESCAPE_ANY_CHAR_VALIDATOR),
        def(ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS_VALIDATOR),
        def(ExecConstants.HIVE_OPTIMIZE_PARQUET_SCAN_WITH_NATIVE_READER_VALIDATOR),
        def(ExecConstants.HIVE_OPTIMIZE_MAPRDB_JSON_SCAN_WITH_NATIVE_READER_VALIDATOR),
        def(ExecConstants.HIVE_READ_MAPRDB_JSON_TIMESTAMP_WITH_TIMEZONE_OFFSET_VALIDATOR),
        def(ExecConstants.HIVE_MAPRDB_JSON_ALL_TEXT_MODE_VALIDATOR),
        def(ExecConstants.HIVE_CONF_PROPERTIES_VALIDATOR),
        def(ExecConstants.SLICE_TARGET_OPTION),
        def(ExecConstants.AFFINITY_FACTOR),
        def(ExecConstants.MAX_WIDTH_GLOBAL),
        def(ExecConstants.MAX_WIDTH_PER_NODE),
        def(ExecConstants.ENABLE_QUEUE),
        def(ExecConstants.LARGE_QUEUE_SIZE),
        def(ExecConstants.QUEUE_THRESHOLD_SIZE),
        def(ExecConstants.QUEUE_TIMEOUT),
        def(ExecConstants.SMALL_QUEUE_SIZE),
        defBuilder(ExecConstants.QUEUE_MEMORY_RESERVE).scopes(SYSTEM).adminOnly().build(),
        defBuilder(ExecConstants.QUEUE_MEMORY_RATIO).scopes(SYSTEM).adminOnly().build(),
        def(ExecConstants.MIN_HASH_TABLE_SIZE),
        def(ExecConstants.MAX_HASH_TABLE_SIZE),
        def(ExecConstants.EARLY_LIMIT0_OPT),
        def(ExecConstants.LATE_LIMIT0_OPT),
        def(ExecConstants.ENABLE_MEMORY_ESTIMATION),
        def(ExecConstants.MAX_QUERY_MEMORY_PER_NODE),
        def(ExecConstants.PERCENT_MEMORY_PER_QUERY),
        def(ExecConstants.MIN_MEMORY_PER_BUFFERED_OP),
        def(ExecConstants.NON_BLOCKING_OPERATORS_MEMORY),
        def(ExecConstants.HASH_JOIN_TABLE_FACTOR),
        defBuilder(ExecOpt.HASH_JOIN_HASHTABLE_CALC_TYPE)
            .scopes(SYSTEM_AND_SESSION).adminOnly().internal().build(),
        defBuilder(ExecOpt.HASH_JOIN_SAFETY_FACTOR)
            .scopes(SYSTEM_AND_SESSION).adminOnly().internal().build(),
        defBuilder(ExecOpt.HASH_JOIN_HASH_DOUBLE_FACTOR)
            .scopes(SYSTEM_AND_SESSION).adminOnly().internal().build(),
        defBuilder(ExecOpt.HASH_JOIN_FRAGMENTATION_FACTOR)
            .scopes(SYSTEM_AND_SESSION).adminOnly().internal().build(),
        def(ExecConstants.HASH_AGG_TABLE_FACTOR),
        def(ExecConstants.AVERAGE_FIELD_WIDTH),
        def(ExecConstants.NEW_VIEW_DEFAULT_PERMS_VALIDATOR),
        def(ExecConstants.CTAS_PARTITIONING_HASH_DISTRIBUTE_VALIDATOR),
        defBuilder(ExecConstants.ADMIN_USERS_VALIDATOR).scopes(SYSTEM).adminOnly().build(),
        defBuilder(ExecConstants.ADMIN_USER_GROUPS_VALIDATOR).scopes(SYSTEM).adminOnly().build(),
        defBuilder(ExecConstants.IMPERSONATION_POLICY_VALIDATOR).scopes(SYSTEM).adminOnly().build(),
        def(ClassCompilerSelector.JAVA_COMPILER_VALIDATOR),
        def(ClassCompilerSelector.JAVA_COMPILER_JANINO_MAXSIZE),
        def(ClassCompilerSelector.JAVA_COMPILER_DEBUG),
        def(ExecConstants.ENABLE_VERBOSE_ERRORS),
        def(ExecConstants.ENABLE_WINDOW_FUNCTIONS_VALIDATOR),
        def(ExecConstants.SCALAR_REPLACEMENT_VALIDATOR),
        def(ExecConstants.ENABLE_NEW_TEXT_READER),
        def(ExecConstants.ENABLE_V3_TEXT_READER),
        def(ExecConstants.SKIP_RUNTIME_ROWGROUP_PRUNING),
        def(ExecConstants.MIN_READER_WIDTH),
        def(ExecConstants.ENABLE_BULK_LOAD_TABLE_LIST),
        def(ExecConstants.BULK_LOAD_TABLE_LIST_BULK_SIZE),
        def(ExecConstants.WEB_LOGS_MAX_LINES_VALIDATOR),
        def(ExecConstants.WEB_DISPLAY_FORMAT_TIMESTAMP_VALIDATOR),
        def(ExecConstants.WEB_DISPLAY_FORMAT_DATE_VALIDATOR),
        def(ExecConstants.WEB_DISPLAY_FORMAT_TIME_VALIDATOR),
        def(ExecConstants.IMPLICIT_FILENAME_COLUMN_LABEL_VALIDATOR),
        def(ExecConstants.IMPLICIT_SUFFIX_COLUMN_LABEL_VALIDATOR),
        def(ExecConstants.IMPLICIT_FQN_COLUMN_LABEL_VALIDATOR),
        def(ExecConstants.IMPLICIT_FILEPATH_COLUMN_LABEL_VALIDATOR),
        def(ExecConstants.IMPLICIT_ROW_GROUP_INDEX_COLUMN_LABEL_VALIDATOR),
        def(ExecConstants.IMPLICIT_ROW_GROUP_START_COLUMN_LABEL_VALIDATOR),
        def(ExecConstants.IMPLICIT_ROW_GROUP_LENGTH_COLUMN_LABEL_VALIDATOR),
        def(ExecConstants.IMPLICIT_LAST_MODIFIED_TIME_COLUMN_LABEL_VALIDATOR),
        def(ExecConstants.IMPLICIT_PROJECT_METADATA_COLUMN_LABEL_VALIDATOR),
        def(ExecConstants.CODE_GEN_EXP_IN_METHOD_SIZE_VALIDATOR),
        def(ExecConstants.CREATE_PREPARE_STATEMENT_TIMEOUT_MILLIS_VALIDATOR),
        defBuilder(ExecConstants.DYNAMIC_UDF_SUPPORT_ENABLED_VALIDATOR).scopes(SYSTEM).adminOnly().build(),
        def(ExecConstants.ENABLE_QUERY_PROFILE_VALIDATOR),
        def(ExecConstants.SKIP_SESSION_QUERY_PROFILE_VALIDATOR),
        def(ExecConstants.QUERY_PROFILE_DEBUG_VALIDATOR),
        def(ExecConstants.USE_DYNAMIC_UDFS),
        def(ExecConstants.QUERY_TRANSIENT_STATE_UPDATE),
        def(ExecConstants.PERSISTENT_TABLE_UMASK_VALIDATOR),
        def(ExecConstants.CPU_LOAD_AVERAGE),
        def(ExecConstants.ENABLE_VECTOR_VALIDATOR),
        def(ExecConstants.ENABLE_ITERATOR_VALIDATOR),
        defBuilder(ExecOpt.OUTPUT_BATCH_SIZE).scopes(SYSTEM).adminOnly().build(),
        defBuilder(ExecConstants.STATS_LOGGING_BATCH_SIZE_VALIDATOR)
            .scopes(SYSTEM_AND_SESSION).adminOnly().internal().build(),
        defBuilder(ExecConstants.STATS_LOGGING_BATCH_FG_SIZE_VALIDATOR)
            .scopes(SYSTEM_AND_SESSION).adminOnly().internal().build(),
        defBuilder(ExecConstants.STATS_LOGGING_BATCH_OPERATOR_VALIDATOR)
            .scopes(SYSTEM_AND_SESSION).adminOnly().internal().build(),
        defBuilder(ExecOpt.OUTPUT_BATCH_SIZE_AVAIL_MEM_FACTOR)
            .scopes(SYSTEM).adminOnly().build(),
        defBuilder(ExecOpt.FRAG_RUNNER_RPC_TIMEOUT)
            .scopes(SYSTEM).adminOnly().internal().build(),
        def(ExecConstants.LIST_FILES_RECURSIVELY_VALIDATOR),
        def(ExecConstants.QUERY_ROWKEYJOIN_BATCHSIZE),
        def(ExecConstants.RETURN_RESULT_SET_FOR_DDL_VALIDATOR),
        def(ExecConstants.HLL_ACCURACY_VALIDATOR),
        def(ExecConstants.DETERMINISTIC_SAMPLING_VALIDATOR),
        def(ExecConstants.NDV_BLOOM_FILTER_ELEMENTS_VALIDATOR),
        def(ExecConstants.NDV_BLOOM_FILTER_FPOS_PROB_VALIDATOR),
        defBuilder(ExecConstants.RM_QUERY_TAGS_VALIDATOR)
            .scopes(SESSION_AND_QUERY).build(),
        def(ExecConstants.RM_QUEUES_WAIT_FOR_PREFERRED_NODES_VALIDATOR),
        def(ExecConstants.TDIGEST_COMPRESSION_VALIDATOR),
        defBuilder(ExecConstants.QUERY_MAX_ROWS_VALIDATOR).adminOnly().build(),
        defBuilder(ExecConstants.QUERY_MAX_ROWS_VALIDATOR).adminOnly().build(),
        def(ExecConstants.METASTORE_ENABLED_VALIDATOR),
        def(ExecConstants.METASTORE_METADATA_STORE_DEPTH_LEVEL_VALIDATOR),
        def(ExecConstants.METASTORE_USE_SCHEMA_METADATA_VALIDATOR),
        def(ExecConstants.METASTORE_USE_STATISTICS_METADATA_VALIDATOR),
        def(ExecConstants.METASTORE_CTAS_AUTO_COLLECT_METADATA_VALIDATOR),
        def(ExecConstants.METASTORE_FALLBACK_TO_FILE_METADATA_VALIDATOR),
        def(ExecConstants.METASTORE_RETRIEVAL_RETRY_ATTEMPTS_VALIDATOR),
        defBuilder(ExecConstants.PARQUET_READER_ENABLE_MAP_SUPPORT_VALIDATOR)
            .scopes(SYSTEM_AND_SESSION).build(),
        def(ExecConstants.ENABLE_DYNAMIC_CREDIT_BASED_FC_VALIDATOR)
    };

    CaseInsensitiveMap<OptionDefinition> map = Arrays.stream(definitions)
        .collect(Collectors.toMap(
            d -> d.getValidator().getOptionName(),
            Function.identity(),
            (o, n) -> n,
            CaseInsensitiveMap::newHashMap));


    if (AssertionUtil.isAssertionsEnabled()) {
      map.put(ExecConstants.DRILLBIT_CONTROL_INJECTIONS, def(ExecConstants.DRILLBIT_CONTROLS_VALIDATOR));
    }

    return map;
  }

  private final PersistentStoreConfig<PersistedOptionValue> config;

  private final PersistentStoreProvider provider;

  /**
   * Persistent store for options that have been changed from default.
   * NOTE: CRUD operations must use lowercase keys.
   */
  private PersistentStore<PersistedOptionValue> options;
  private final CaseInsensitiveMap<OptionDefinition> definitions;
  private final CaseInsensitiveMap<OptionValue> defaults;

  public SystemOptionManager(LogicalPlanPersistence lpPersistence, final PersistentStoreProvider provider,
                             final DrillConfig bootConfig) {
    this(lpPersistence, provider, bootConfig, SystemOptionManager.createDefaultOptionDefinitions());
  }

  public SystemOptionManager(final LogicalPlanPersistence lpPersistence, final PersistentStoreProvider provider,
                             final DrillConfig bootConfig, final CaseInsensitiveMap<OptionDefinition> definitions) {
    this.provider = provider;
    this.config = PersistentStoreConfig.newJacksonBuilder(lpPersistence.getMapper(), PersistedOptionValue.class)
        .name("sys.options")
        .build();
    this.definitions = definitions;
    this.defaults = populateDefaultValues(definitions, bootConfig);
  }

  /**
   * Test-only, in-memory version of the system option manager.
   *
   * @param bootConfig Drill config
   */
  @VisibleForTesting
  public SystemOptionManager(final DrillConfig bootConfig) {
    this.provider = new InMemoryStoreProvider(100);
    this.config = null;
    this.definitions = SystemOptionManager.createDefaultOptionDefinitions();
    this.defaults = populateDefaultValues(definitions, bootConfig);
  }

  /**
   * Initializes this option manager.
   *
   * @return this option manager
   * @throws Exception if unable to initialize option manager
   */
  public SystemOptionManager init() throws Exception {
    options = provider.getOrCreateStore(config);
    // if necessary, deprecate and replace options from persistent store
    for (final Entry<String, PersistedOptionValue> option : Lists.newArrayList(options.getAll())) {
      final String name = option.getKey();
      final OptionDefinition definition = definitions.get(name);
      if (definition == null) {
        // deprecated option, delete.
        options.delete(name);
        logger.warn("Deleting deprecated option `{}`", name);
      } else {
        OptionValidator validator = definition.getValidator();
        final String canonicalName = validator.getOptionName().toLowerCase();
        if (!name.equals(canonicalName)) {
          // for backwards compatibility <= 1.1, rename to lower case.
          logger.warn("Changing option name to lower case `{}`", name);
          final PersistedOptionValue value = option.getValue();
          options.delete(name);
          options.put(canonicalName, value);
        }
      }
    }

    return this;
  }

  @Override
  public Iterator<OptionValue> iterator() {
    final Map<String, OptionValue> buildList = CaseInsensitiveMap.newHashMap();
    // populate the default options
    for (final Map.Entry<String, OptionValue> entry : defaults.entrySet()) {
      buildList.put(entry.getKey(), entry.getValue());
    }
    // override if changed
    for (final Map.Entry<String, PersistedOptionValue> entry : Lists.newArrayList(options.getAll())) {
      final String name = entry.getKey();
      final OptionDefinition optionDefinition = getOptionDefinition(name);
      final PersistedOptionValue persistedOptionValue = entry.getValue();
      final OptionValue optionValue = persistedOptionValue
          .toOptionValue(optionDefinition, OptionValue.OptionScope.SYSTEM);
      buildList.put(name, optionValue);
    }
    return buildList.values().iterator();
  }

  @Override
  public OptionValue getOption(final String name) {
    // check local space (persistent store)
    final PersistedOptionValue persistedValue = options.get(name.toLowerCase());

    if (persistedValue != null) {
      final OptionDefinition optionDefinition = getOptionDefinition(name);
      return persistedValue.toOptionValue(optionDefinition, OptionValue.OptionScope.SYSTEM);
    }

    // otherwise, return default set in the validator.
    return defaults.get(name);
  }

  @Override
  public OptionValue getDefault(String optionName) {
    OptionValue value = defaults.get(optionName);
    if (value == null) {
      throw UserException.systemError(null)
          .addContext("Undefined default value for option: " + optionName)
          .build(logger);
    }
    return value;
  }

  @Override
  protected void setLocalOptionHelper(final OptionValue value) {
    final String name = value.name.toLowerCase();
    final OptionDefinition definition = getOptionDefinition(name);
    final OptionValidator validator = definition.getValidator();

    validator.validate(value, definition.getMetaData(), this); // validate the option

    if (options.get(name) == null && value.equals(getDefault(name))) {
      return; // if the option is not overridden, ignore setting option to default
    }

    options.put(name, value.toPersisted());
  }

  @Override
  protected OptionValue.OptionScope getScope() {
    return OptionValue.OptionScope.SYSTEM;
  }

  @Override
  public void deleteLocalOption(final String name) {
    getOptionDefinition(name); // ensure option exists
    options.delete(name.toLowerCase());
  }

  @Override
  public void deleteAllLocalOptions() {
    Iterable<Map.Entry<String, PersistedOptionValue>> allOptions = () -> options.getAll();
    StreamSupport.stream(allOptions.spliterator(), false)
        .map(Entry::getKey)
        .forEach(name -> options.delete(name)); // should be lowercase
  }

  private CaseInsensitiveMap<OptionValue> populateDefaultValues(Map<String, OptionDefinition> definitions, DrillConfig bootConfig) {
    // populate the options from the config
    final Map<String, OptionValue> defaults = new HashMap<>();

    for (final Map.Entry<String, OptionDefinition> entry : definitions.entrySet()) {
      final OptionDefinition definition = entry.getValue();
      final OptionMetaData metaData = definition.getMetaData();
      final OptionValue.AccessibleScopes type = metaData.getAccessibleScopes();
      final OptionValidator validator = definition.getValidator();
      final String name = validator.getOptionName();
      final String configName = validator.getConfigProperty();
      final OptionValue.Kind kind = validator.getKind();
      OptionValue optionValue;

      switch (kind) {
        case BOOLEAN:
          optionValue = OptionValue.create(type, name,
              bootConfig.getBoolean(configName), OptionValue.OptionScope.BOOT);
          break;
        case LONG:
          optionValue = OptionValue.create(type, name,
              bootConfig.getLong(configName), OptionValue.OptionScope.BOOT);
          break;
        case STRING:
          optionValue = OptionValue.create(type, name,
              bootConfig.getString(configName), OptionValue.OptionScope.BOOT);
          break;
        case DOUBLE:
          optionValue = OptionValue.create(type, name,
              bootConfig.getDouble(configName), OptionValue.OptionScope.BOOT);
          break;
        default:
          throw new UnsupportedOperationException();
      }

      defaults.put(name, optionValue);
    }

    return CaseInsensitiveMap.newImmutableMap(defaults);
  }

  /**
   * Gets the {@link OptionDefinition} associated with the name.
   *
   * @param name name of the option
   * @return the associated option definition
   * @throws UserException - if the definition is not found
   */
  @Override
  public OptionDefinition getOptionDefinition(String name) {
    final OptionDefinition definition = definitions.get(name);
    if (definition == null) {
      throw UserException.validationError()
          .message(String.format("The option '%s' does not exist.", name.toLowerCase()))
          .build(logger);
    }
    return definition;
  }

  @Override
  public OptionList getOptionList() {
    return (OptionList) IteratorUtils.toList(iterator());
  }

  @Override
  public void close() throws Exception {
    // If the server exits very early, the options may not yet have
    // been created. Gracefully handle that case.

    if (options != null) {
      options.close();
    }
  }
}
