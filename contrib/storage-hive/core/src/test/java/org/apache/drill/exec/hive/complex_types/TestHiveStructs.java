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
import java.util.Map;

import org.apache.drill.exec.hive.HiveTestFixture;
import org.apache.drill.exec.hive.HiveTestUtilities;
import org.apache.drill.exec.util.Text;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.hadoop.hive.ql.Driver;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static java.util.Objects.nonNull;

public class TestHiveStructs extends ClusterTest {

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
    hiveTestFixture.getDriverManager().runWithinSession(TestHiveStructs::generateData);

    // Use plugin manager to add, remove, update hive storage plugin of one or many test drillbits
    hiveTestFixture.getPluginManager().addHivePluginTo(cluster.drillbits());
  }

  private static void generateData(Driver d) {
    HiveTestUtilities.executeQuery(d, "CREATE TABLE str_primitives(car STRUCT<name:STRING, age:INT>) " +
        "ROW FORMAT DELIMITED COLLECTION ITEMS TERMINATED BY ',' NULL DEFINED AS 'x' STORED AS TEXTFILE");
    // Set up data using HiveTestUtilities.executeQuery(driver, sql)
    HiveTestUtilities.loadData(d, "str_primitives", Paths.get("complex_types/struct/primitives_struct.txt"));

    HiveTestUtilities.executeQuery(d,
        "CREATE TABLE nullstr(str STRUCT<f1:INT>) ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE");
    HiveTestUtilities.loadData(d, "nullstr", Paths.get("complex_types/struct/nullstruct.json"));
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (nonNull(hiveTestFixture)) {
      hiveTestFixture.getPluginManager().removeHivePluginFrom(cluster.drillbits());
    }
  }

  @Test
  public void structWithPrimitiveTypes() throws Exception {
    testBuilder()
        .sqlQuery("SELECT car FROM hive.`str_primitives`")
        .unOrdered()
        .baselineColumns("car")
        .baselineValues(MapBuilder.startMap().put("name", new Text("Skoda")).put("age", 5).get())
        .baselineValues(MapBuilder.startMap().put("name", new Text("BMWda")).put("age", 1).get()) // todo: not ok , value should be BMW not BMWda
        .baselineValues(MapBuilder.startMap().put("name", new Text("Renault")).put("age", 3).get())
        .go();
  }

  @Test
  public void structWithPrimitiveTypesByKey() throws Exception {
        testBuilder()
        .sqlQuery("SELECT car['age'] FROM hive.`str_primitives`")
        .unOrdered()
        .baselineColumns("EXPR$0") //todo: fix the name to be car.age or car['age']
        .baselineValues(5)
        .baselineValues(1)
        .baselineValues(3)
        .go();
  }

  @Test
  public void structWithPrimitiveTypesNullability() throws Exception {
    /*
       case 1: struct is null
       case 2: value is null
    */
    testBuilder()
        .sqlQuery("SELECT str FROM hive.`nullstr`")
        .unOrdered()
        .baselineColumns("str")
        .baselineValues((Map) null)
        .baselineValues(MapBuilder.startMap().put("f1", null).get())
        .go();
  }


//  @Test
//  public void structInJson() throws Exception {
//    testBuilder()
//        .sqlQuery("SELECT es FROM cp.`easy_struct.json`")
//        .unOrdered()
//        .baselineColumns("es")
//        .baselineValues(MapBuilder.startMap().put("id", 1).put("str", "First Row").get())
//        .baselineValues(MapBuilder.startMap().put("id", 2).put("str", "Second Row").get())
//        .baselineValues(MapBuilder.startMap().put("id", 3).put("str", "Third Row").get())
//        .go();
//  }

  // todo: add tests for nesting of complex types

}
