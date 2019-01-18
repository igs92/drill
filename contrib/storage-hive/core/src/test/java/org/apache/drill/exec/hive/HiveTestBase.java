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
package org.apache.drill.exec.hive;

import java.io.File;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.drill.PlanTestBase;
import org.apache.drill.exec.store.hive.HiveTestDataGenerator;
import org.apache.drill.test.BaseDirTestWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.Description;

/**
 * Base class for Hive test. Takes care of generating and adding Hive test plugin before tests and deleting the
 * plugin after tests.
 */
public class HiveTestBase extends PlanTestBase {

  public static final HiveTestFixture HIVE_TEST_FIXTURE;

  static {
    // generate hive data common for all test classes using own dirWatcher
    BaseDirTestWatcher generalDirWatcher = new BaseDirTestWatcher() {
      {
        String uniqueSuffix = String.format("_%s", UUID.randomUUID().toString().replace("-", ""));
        starting(Description.createSuiteDescription(HiveTestBase.class.getName().concat(uniqueSuffix)));
      }
    };
    File baseDir = generalDirWatcher.getRootDir();
    HIVE_TEST_FIXTURE = HiveTestFixture.builder(baseDir).build();
    HiveTestDataGenerator dataGenerator = new HiveTestDataGenerator(generalDirWatcher, baseDir,
        HIVE_TEST_FIXTURE.getWarehouseDir());
    HIVE_TEST_FIXTURE.getDriverManager().doWithinSession(dataGenerator::generateData);

    // set hook for clearing watcher's dir on JVM shutdown
    Runtime.getRuntime().addShutdownHook(new Thread(() -> FileUtils.deleteQuietly(generalDirWatcher.getDir())));
  }

  @BeforeClass
  public static void setUp() {
    HIVE_TEST_FIXTURE.getPluginManager().addHivePluginTo(bits);
  }

  @AfterClass
  public static void tearDown() {
    if (HIVE_TEST_FIXTURE != null) {
      HIVE_TEST_FIXTURE.getPluginManager().removeHivePluginFrom(bits);
    }
  }

}
