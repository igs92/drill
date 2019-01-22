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
package org.apache.drill.exec.hive.complex_types;

import java.nio.file.Paths;

import org.apache.drill.exec.hive.HiveTestFixture;
import org.apache.drill.exec.hive.HiveTestUtilities;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.hadoop.hive.ql.Driver;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static java.util.Objects.nonNull;

public class TestHiveUnions extends ClusterTest {

  private static HiveTestFixture hiveTestFixture;

  @BeforeClass
  public static void setUp() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher)
        .configProperty("drill.exec.rpc.user.timeout", 9999)
        .configProperty("drill.exec.rpc.bit.timeout", 9999)
    );

    // Below is minimal config which uses defaults from HiveTestFixture.Builder
    // constructor, but any option for driver or storage plugin may be
    // overridden using builder's methods
    hiveTestFixture = HiveTestFixture.builder(dirTestWatcher).build();

    // Use driver manager to configure test data in Hive metastore
    hiveTestFixture.getDriverManager().runWithinSession(TestHiveUnions::generateData);

    // Use plugin manager to add, remove, update hive storage plugin of one or many test drillbits
    hiveTestFixture.getPluginManager().addHivePluginTo(cluster.drillbits());
  }

  private static void generateData(Driver driver) {
    HiveTestUtilities.executeQuery(driver, "CREATE TABLE primitives_union(u_data UNIONTYPE<STRING, INT>) " +
        "ROW FORMAT DELIMITED COLLECTION ITEMS TERMINATED BY ',' NULL DEFINED AS 'x' STORED AS TEXTFILE");
    // Set up data using HiveTestUtilities.executeQuery(driver, sql)
    HiveTestUtilities.loadData(driver, "primitives_union", Paths.get("complex_types/struct/primitives_struct.txt"));

  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (nonNull(hiveTestFixture)) {
      hiveTestFixture.getPluginManager().removeHivePluginFrom(cluster.drillbits());
    }
  }

  @Test
  public void unionWithPrimitiveTypes() throws Exception {
//    testBuilder()
//        .sqlQuery("SELECT car FROM hive.`str_primitives`")
//        .unOrdered()
//        .baselineColumns("car")
//        .baselineValues(MapBuilder.startMap("Skoda", 5).get())
//        .baselineValues(MapBuilder.startMap("BMW", 1).get())
//        .baselineValues(MapBuilder.startMap("Renault", 3).get())
//        .go();

  }

  @Test
  public void unionWithComplexTypes() throws Exception {
  }

  @Test
  public void unionWithPrimitiveAndComplexTypes() throws Exception {

  }
}
