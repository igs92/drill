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
import java.util.Arrays;
import java.util.List;

import org.apache.drill.categories.HiveStorageTest;
import org.apache.drill.exec.hive.HiveTestFixture;
import org.apache.drill.exec.hive.HiveTestUtilities;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.hadoop.hive.ql.Driver;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({HiveStorageTest.class})
public class TestHiveArrays extends ClusterTest {

  private static HiveTestFixture hiveTestFixture;

  @BeforeClass
  public static void setUp() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher)
        .configProperty("drill.exec.rpc.user.timeout", 9999)
        .configProperty("drill.exec.rpc.bit.timeout", 9999)
    );
    hiveTestFixture = HiveTestFixture.builder(dirTestWatcher).build();
    hiveTestFixture.getDriverManager().runWithinSession(TestHiveArrays::generateData);
    hiveTestFixture.getPluginManager().addHivePluginTo(cluster.drillbit());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (hiveTestFixture != null) {
      hiveTestFixture.getPluginManager().removeHivePluginFrom(cluster.drillbit());
    }
  }

  private static void generateData(Driver d) {
    // int_arr_text
    HiveTestUtilities.executeQuery(d,
        "CREATE TABLE int_arr_text(arr ARRAY<INT>) ROW FORMAT DELIMITED COLLECTION ITEMS TERMINATED BY ',' NULL DEFINED AS 'x' STORED AS TEXTFILE");
    HiveTestUtilities.loadData(d, "int_arr_text", Paths.get("complex_types/array/int_arr.txt"));
    // int_arr_nested_1_json
    HiveTestUtilities.executeQuery(d,
        "CREATE TABLE int_arr_nested_1_json(arr ARRAY<ARRAY<INT>>) ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE");
    HiveTestUtilities.loadData(d, "int_arr_nested_1_json", Paths.get("complex_types/array/int_arr_nested_1.json"));
  }

  @Test
  public void intArrayInSequence() throws Exception {

  }

  @Test
  public void intArrayInText() throws Exception {
    testBuilder()
        .sqlQuery("SELECT arr FROM hive.`int_arr_text`")
        .unOrdered()
        .baselineColumns("arr")
        .baselineValuesForSingleColumn(Arrays.asList(-2147483648, null))
        .baselineValuesForSingleColumn(Arrays.asList(-2147483648, 2147483647))
        .baselineValuesForSingleColumn(Arrays.asList(0, 1, 3))
        .baselineValuesForSingleColumn((List) null)
        .baselineValuesForSingleColumn(Arrays.asList(2, 2))
        .go();
  }

  @Test
  public void intArrayInParquet() throws Exception {

  }

  @Test
  public void arrayOfIntArraysInSequence() throws Exception {
  }

  @Test
  public void arrayOfIntArraysInText() throws Exception {
    testBuilder()
        .sqlQuery("SELECT arr FROM hive.`int_arr_nested_1_json`")
        .unOrdered()
        .baselineColumns("arr")
        .baselineValuesForSingleColumn(Arrays.asList(
            Arrays.asList(1, 2, 3),
            Arrays.asList(4, 5, 6),
            Arrays.asList(7, 8, 9)
        ))
        .baselineValuesForSingleColumn(Arrays.asList(
            Arrays.asList(10, 11, 12),
            Arrays.asList(13, 14, 15),
            Arrays.asList(16, 17, 18)
        ))
        .baselineValuesForSingleColumn(Arrays.asList(
            Arrays.asList(19),
            Arrays.asList(20)
        ))
        .go();
  }

  @Test
  public void arrayOfIntArraysInParquet() throws Exception {
  }


//  ------------------------------------------------------------------------------------------

  @Test
  public void stringArray() throws Exception {

  }

  @Test
  public void varcharArray() throws Exception {
  }

  @Test
  public void charArray() throws Exception {
  }

  @Test
  public void byteArray() throws Exception {
  }

  @Test
  public void shortArray() throws Exception {
  }

  @Test
  public void decimalArray() throws Exception {
  }

  @Test
  public void booleanArray() throws Exception {
  }

  @Test
  public void longArray() throws Exception {
  }

  @Test
  public void floatArray() throws Exception {
  }

  @Test
  public void doubleArray() throws Exception {
  }

  @Test
  public void dateArray() throws Exception {
  }

  @Test
  public void timestampArray() throws Exception {
  }

  @Test
  public void binaryArray() throws Exception {
  }

  @Test
  public void arrayOfStringArrays() throws Exception {
  }

  @Test
  public void arrayOfVarcharArrays() throws Exception {
  }

  @Test
  public void arrayOfCharArrays() throws Exception {
  }

  @Test
  public void arrayOfByteArrays() throws Exception {
  }

  @Test
  public void arrayOfShortArrays() throws Exception {
  }

  @Test
  public void arrayOfDecimalArrays() throws Exception {
  }

  @Test
  public void arrayOfBooleanArrays() throws Exception {
  }

  @Test
  public void arrayOfLongArrays() throws Exception {
  }

  @Test
  public void arrayOfFloatArrays() throws Exception {
  }

  @Test
  public void arrayOfDoubleArrays() throws Exception {
  }

  @Test
  public void arrayOfDateArrays() throws Exception {
  }

  @Test
  public void arrayOfTimestampArrays() throws Exception {
  }

  @Test
  public void arrayOfBinaryArrays() throws Exception {
  }

  @Test
  public void arrayOfStructWithAllDataTypes() throws Exception {
  }

  @Test
  public void arrayOfUnionWithAllDataTypes() throws Exception {
  }


  @Test
  public void arrayOfMapStringToStringArray() {
  }

  @Test
  public void arrayOfMapDoubleToIntArray() {
  }

  @Test
  public void arrayOfMapTimestampToFloatArray() {
  }

  @Test
  public void arrayOfMapIntToVarchar() throws Exception {
  }

  @Test
  public void arrayOfMapFloatToChar() throws Exception {
  }

  @Test
  public void arrayOfMapDecimalToByte() throws Exception {
  }

  @Test
  public void arrayOfMapBooleanToShort() throws Exception {
  }

  @Test
  public void arrayOfMapDateToInt() throws Exception {
  }

  @Test
  public void arrayOfMapStringToDecimal() throws Exception {
  }

  @Test
  public void arrayOfMapCharToBoolean() throws Exception {
  }

  @Test
  public void arrayOfMapTimestampToLong() throws Exception {
  }

  @Test
  public void arrayOfMapLongToFloat() throws Exception {
  }

  @Test
  public void arrayOfMapStringToDouble() throws Exception {
  }

  @Test
  public void arrayOfMapBinaryToDate() throws Exception {
  }

  @Test
  public void arrayOfMapByteToTimestamp() throws Exception {
  }

  @Test
  public void arrayOfMapVarcharToBinary() throws Exception {
  }

}
