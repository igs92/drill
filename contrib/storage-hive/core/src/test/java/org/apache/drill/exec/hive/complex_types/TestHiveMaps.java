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

import static org.apache.drill.exec.hive.HiveTestUtilities.executeQuery;

@Category({HiveStorageTest.class})
public class TestHiveMaps extends ClusterTest {

  private static HiveTestFixture hiveTestFixture;

  @BeforeClass
  public static void setUp() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher)
        .configProperty("drill.exec.rpc.user.timeout", 9999)
        .configProperty("drill.exec.rpc.bit.timeout", 9999)
    );
    hiveTestFixture = HiveTestFixture.builder(dirTestWatcher).build();
    hiveTestFixture.getDriverManager().runWithinSession(TestHiveMaps::generateData);
    hiveTestFixture.getPluginManager().addHivePluginTo(cluster.drillbit());
  }

  private static void generateData(Driver driver) {
    createTable(driver, "int_int_map_text", "MAP<INT,INT>","TEXTFILE");
    HiveTestUtilities.loadData(driver, "int_int_map_text", Paths.get("complex_types/map/int_int_map_text.txt"));
  }

  private static void createTable(Driver driver, String tableName, String mapType, String storeFormat){
    String sqlToRun = String.format(
        "CREATE TABLE %s(maq %s) ROW FORMAT DELIMITED COLLECTION ITEMS TERMINATED BY '|' MAP KEYS TERMINATED BY '>' STORED AS %s",
        tableName, mapType, storeFormat);
    executeQuery(driver, sqlToRun);
  }



  @AfterClass
  public static void tearDown() throws Exception {
    if (hiveTestFixture != null) {
      hiveTestFixture.getPluginManager().removeHivePluginFrom(cluster.drillbit());
    }
  }

  @Test
  public void mapIntToInt() throws Exception {
    testBuilder()
        .sqlQuery("SELECT maq FROM hive.`int_int_map_text`")
        .unOrdered()
        .baselineColumns("maq")
        .baselineValuesForSingleColumn(MapBuilder.startMap(1, 10).put(2, 20).put(3, 30).get())
        .baselineValuesForSingleColumn(MapBuilder.startMap(1, 20).put(2, 40).put(3, 60).get())
        .baselineValuesForSingleColumn(MapBuilder.startMap(1, 0).put(2, 0).get())
        .go();
  }



  @Test
  public void mapIntToString() throws Exception {
  }

  @Test
  public void mapIntToVarchar() throws Exception {
  }

  @Test
  public void mapIntToChar() throws Exception {
  }

  @Test
  public void mapIntToByte() throws Exception {
  }

  @Test
  public void mapIntToShort() throws Exception {
  }

  @Test
  public void mapIntToDecimal() throws Exception {
  }

  @Test
  public void mapIntToBoolean() throws Exception {
  }

  @Test
  public void mapIntToLong() throws Exception {
  }

  @Test
  public void mapIntToFloat() throws Exception {
  }

  @Test
  public void mapIntToDouble() throws Exception {
  }

  @Test
  public void mapIntToDate() throws Exception {
  }

  @Test
  public void mapIntToTimestamp() throws Exception {
  }

  @Test
  public void mapIntToBinary() throws Exception {
  }

  @Test
  public void mapStringToString() throws Exception {
  }

  @Test
  public void mapStringToVarchar() throws Exception {
  }

  @Test
  public void mapStringToChar() throws Exception {
  }

  @Test
  public void mapStringToByte() throws Exception {
  }

  @Test
  public void mapStringToShort() throws Exception {
  }

  @Test
  public void mapStringToInt() throws Exception {
  }

  @Test
  public void mapStringToDecimal() throws Exception {
  }

  @Test
  public void mapStringToBoolean() throws Exception {
  }

  @Test
  public void mapStringToLong() throws Exception {
  }

  @Test
  public void mapStringToFloat() throws Exception {
  }

  @Test
  public void mapStringToDouble() throws Exception {
  }

  @Test
  public void mapStringToDate() throws Exception {
  }

  @Test
  public void mapStringToTimestamp() throws Exception {
  }

  @Test
  public void mapStringToBinary() throws Exception {
  }

  @Test
  public void mapVarcharToString() throws Exception {
  }

  @Test
  public void mapVarcharToVarchar() throws Exception {
  }

  @Test
  public void mapVarcharToChar() throws Exception {
  }

  @Test
  public void mapVarcharToByte() throws Exception {
  }

  @Test
  public void mapVarcharToShort() throws Exception {
  }

  @Test
  public void mapVarcharToInt() throws Exception {
  }

  @Test
  public void mapVarcharToDecimal() throws Exception {
  }

  @Test
  public void mapVarcharToBoolean() throws Exception {
  }

  @Test
  public void mapVarcharToLong() throws Exception {
  }

  @Test
  public void mapVarcharToFloat() throws Exception {
  }

  @Test
  public void mapVarcharToDouble() throws Exception {
  }

  @Test
  public void mapVarcharToDate() throws Exception {
  }

  @Test
  public void mapVarcharToTimestamp() throws Exception {
  }

  @Test
  public void mapVarcharToBinary() throws Exception {
  }

  @Test
  public void mapCharToString() throws Exception {
  }

  @Test
  public void mapCharToVarchar() throws Exception {
  }

  @Test
  public void mapCharToChar() throws Exception {
  }

  @Test
  public void mapCharToByte() throws Exception {
  }

  @Test
  public void mapCharToShort() throws Exception {
  }

  @Test
  public void mapCharToInt() throws Exception {
  }

  @Test
  public void mapCharToDecimal() throws Exception {
  }

  @Test
  public void mapCharToBoolean() throws Exception {
  }

  @Test
  public void mapCharToLong() throws Exception {
  }

  @Test
  public void mapCharToFloat() throws Exception {
  }

  @Test
  public void mapCharToDouble() throws Exception {
  }

  @Test
  public void mapCharToDate() throws Exception {
  }

  @Test
  public void mapCharToTimestamp() throws Exception {
  }

  @Test
  public void mapCharToBinary() throws Exception {
  }


  @Test
  public void mapByteToString() throws Exception {
  }

  @Test
  public void mapByteToVarchar() throws Exception {
  }

  @Test
  public void mapByteToChar() throws Exception {
  }

  @Test
  public void mapByteToByte() throws Exception {
  }

  @Test
  public void mapByteToShort() throws Exception {
  }

  @Test
  public void mapByteToInt() throws Exception {
  }

  @Test
  public void mapByteToDecimal() throws Exception {
  }

  @Test
  public void mapByteToBoolean() throws Exception {
  }

  @Test
  public void mapByteToLong() throws Exception {
  }

  @Test
  public void mapByteToFloat() throws Exception {
  }

  @Test
  public void mapByteToDouble() throws Exception {
  }

  @Test
  public void mapByteToDate() throws Exception {
  }

  @Test
  public void mapByteToTimestamp() throws Exception {
  }

  @Test
  public void mapByteToBinary() throws Exception {
  }

  @Test
  public void mapShortToString() throws Exception {
  }

  @Test
  public void mapShortToVarchar() throws Exception {
  }

  @Test
  public void mapShortToChar() throws Exception {
  }

  @Test
  public void mapShortToByte() throws Exception {
  }

  @Test
  public void mapShortToShort() throws Exception {
  }

  @Test
  public void mapShortToInt() throws Exception {
  }

  @Test
  public void mapShortToDecimal() throws Exception {
  }

  @Test
  public void mapShortToBoolean() throws Exception {
  }

  @Test
  public void mapShortToLong() throws Exception {
  }

  @Test
  public void mapShortToFloat() throws Exception {
  }

  @Test
  public void mapShortToDouble() throws Exception {
  }

  @Test
  public void mapShortToDate() throws Exception {
  }

  @Test
  public void mapShortToTimestamp() throws Exception {
  }

  @Test
  public void mapShortToBinary() throws Exception {
  }




  @Test
  public void mapDecimalToString() throws Exception {
  }

  @Test
  public void mapDecimalToVarchar() throws Exception {
  }

  @Test
  public void mapDecimalToChar() throws Exception {
  }

  @Test
  public void mapDecimalToByte() throws Exception {
  }

  @Test
  public void mapDecimalToShort() throws Exception {
  }

  @Test
  public void mapDecimalToInt() throws Exception {
  }

  @Test
  public void mapDecimalToDecimal() throws Exception {
  }

  @Test
  public void mapDecimalToBoolean() throws Exception {
  }

  @Test
  public void mapDecimalToLong() throws Exception {
  }

  @Test
  public void mapDecimalToFloat() throws Exception {
  }

  @Test
  public void mapDecimalToDouble() throws Exception {
  }

  @Test
  public void mapDecimalToDate() throws Exception {
  }

  @Test
  public void mapDecimalToTimestamp() throws Exception {
  }

  @Test
  public void mapDecimalToBinary() throws Exception {
  }

  @Test
  public void mapBooleanToString() throws Exception {
  }

  @Test
  public void mapBooleanToVarchar() throws Exception {
  }

  @Test
  public void mapBooleanToChar() throws Exception {
  }

  @Test
  public void mapBooleanToByte() throws Exception {
  }

  @Test
  public void mapBooleanToShort() throws Exception {
  }

  @Test
  public void mapBooleanToInt() throws Exception {
  }

  @Test
  public void mapBooleanToDecimal() throws Exception {
  }

  @Test
  public void mapBooleanToBoolean() throws Exception {
  }

  @Test
  public void mapBooleanToLong() throws Exception {
  }

  @Test
  public void mapBooleanToFloat() throws Exception {
  }

  @Test
  public void mapBooleanToDouble() throws Exception {
  }

  @Test
  public void mapBooleanToDate() throws Exception {
  }

  @Test
  public void mapBooleanToTimestamp() throws Exception {
  }

  @Test
  public void mapBooleanToBinary() throws Exception {
  }

  @Test
  public void mapLongToString() throws Exception {
  }

  @Test
  public void mapLongToVarchar() throws Exception {
  }

  @Test
  public void mapLongToChar() throws Exception {
  }

  @Test
  public void mapLongToByte() throws Exception {
  }

  @Test
  public void mapLongToShort() throws Exception {
  }

  @Test
  public void mapLongToInt() throws Exception {
  }

  @Test
  public void mapLongToDecimal() throws Exception {
  }

  @Test
  public void mapLongToBoolean() throws Exception {
  }

  @Test
  public void mapLongToLong() throws Exception {
  }

  @Test
  public void mapLongToFloat() throws Exception {
  }

  @Test
  public void mapLongToDouble() throws Exception {
  }

  @Test
  public void mapLongToDate() throws Exception {
  }

  @Test
  public void mapLongToTimestamp() throws Exception {
  }

  @Test
  public void mapLongToBinary() throws Exception {
  }

  @Test
  public void mapFloatToString() throws Exception {
  }

  @Test
  public void mapFloatToVarchar() throws Exception {
  }

  @Test
  public void mapFloatToChar() throws Exception {
  }

  @Test
  public void mapFloatToByte() throws Exception {
  }

  @Test
  public void mapFloatToShort() throws Exception {
  }

  @Test
  public void mapFloatToInt() throws Exception {
  }

  @Test
  public void mapFloatToDecimal() throws Exception {
  }

  @Test
  public void mapFloatToBoolean() throws Exception {
  }

  @Test
  public void mapFloatToLong() throws Exception {
  }

  @Test
  public void mapFloatToFloat() throws Exception {
  }

  @Test
  public void mapFloatToDouble() throws Exception {
  }

  @Test
  public void mapFloatToDate() throws Exception {
  }

  @Test
  public void mapFloatToTimestamp() throws Exception {
  }

  @Test
  public void mapFloatToBinary() throws Exception {
  }

  @Test
  public void mapDoubleToString() throws Exception {
  }

  @Test
  public void mapDoubleToVarchar() throws Exception {
  }

  @Test
  public void mapDoubleToChar() throws Exception {
  }

  @Test
  public void mapDoubleToByte() throws Exception {
  }

  @Test
  public void mapDoubleToShort() throws Exception {
  }

  @Test
  public void mapDoubleToInt() throws Exception {
  }

  @Test
  public void mapDoubleToDecimal() throws Exception {
  }

  @Test
  public void mapDoubleToBoolean() throws Exception {
  }

  @Test
  public void mapDoubleToLong() throws Exception {
  }

  @Test
  public void mapDoubleToFloat() throws Exception {
  }

  @Test
  public void mapDoubleToDouble() throws Exception {
  }

  @Test
  public void mapDoubleToDate() throws Exception {
  }

  @Test
  public void mapDoubleToTimestamp() throws Exception {
  }

  @Test
  public void mapDoubleToBinary() throws Exception {
  }

  @Test
  public void mapDateToString() throws Exception {
  }

  @Test
  public void mapDateToVarchar() throws Exception {
  }

  @Test
  public void mapDateToChar() throws Exception {
  }

  @Test
  public void mapDateToByte() throws Exception {
  }

  @Test
  public void mapDateToShort() throws Exception {
  }

  @Test
  public void mapDateToInt() throws Exception {
  }

  @Test
  public void mapDateToDecimal() throws Exception {
  }

  @Test
  public void mapDateToBoolean() throws Exception {
  }

  @Test
  public void mapDateToLong() throws Exception {
  }

  @Test
  public void mapDateToFloat() throws Exception {
  }

  @Test
  public void mapDateToDouble() throws Exception {
  }

  @Test
  public void mapDateToDate() throws Exception {
  }

  @Test
  public void mapDateToTimestamp() throws Exception {
  }

  @Test
  public void mapDateToBinary() throws Exception {
  }

  @Test
  public void mapTimestampToString() throws Exception {
  }

  @Test
  public void mapTimestampToVarchar() throws Exception {
  }

  @Test
  public void mapTimestampToChar() throws Exception {
  }

  @Test
  public void mapTimestampToByte() throws Exception {
  }

  @Test
  public void mapTimestampToShort() throws Exception {
  }

  @Test
  public void mapTimestampToInt() throws Exception {
  }

  @Test
  public void mapTimestampToDecimal() throws Exception {
  }

  @Test
  public void mapTimestampToBoolean() throws Exception {
  }

  @Test
  public void mapTimestampToLong() throws Exception {
  }

  @Test
  public void mapTimestampToFloat() throws Exception {
  }

  @Test
  public void mapTimestampToDouble() throws Exception {
  }

  @Test
  public void mapTimestampToDate() throws Exception {
  }

  @Test
  public void mapTimestampToTimestamp() throws Exception {
  }

  @Test
  public void mapTimestampToBinary() throws Exception {
  }

  @Test
  public void mapBinaryToString() throws Exception {
  }

  @Test
  public void mapBinaryToVarchar() throws Exception {
  }

  @Test
  public void mapBinaryToChar() throws Exception {
  }

  @Test
  public void mapBinaryToByte() throws Exception {
  }

  @Test
  public void mapBinaryToShort() throws Exception {
  }

  @Test
  public void mapBinaryToInt() throws Exception {
  }

  @Test
  public void mapBinaryToDecimal() throws Exception {
  }

  @Test
  public void mapBinaryToBoolean() throws Exception {
  }

  @Test
  public void mapBinaryToLong() throws Exception {
  }

  @Test
  public void mapBinaryToFloat() throws Exception {
  }

  @Test
  public void mapBinaryToDouble() throws Exception {
  }

  @Test
  public void mapBinaryToDate() throws Exception {
  }

  @Test
  public void mapBinaryToTimestamp() throws Exception {
  }

  @Test
  public void mapBinaryToBinary() throws Exception {
  }


  @Test
  public void mapStringToArrayOfBinary() throws Exception {
  }

  @Test
  public void mapVarcharToArrayOfTimestamp() throws Exception {
  }

  @Test
  public void mapCharToArrayOfDate() throws Exception {
  }

  @Test
  public void mapByteToArrayOfDouble() throws Exception {
  }

  @Test
  public void mapShortToArrayOfFloat() throws Exception {
  }

  @Test
  public void mapIntToArrayOfLong() throws Exception {
  }

  @Test
  public void mapDecimalToArrayOfBoolean() throws Exception {
  }

  @Test
  public void mapBooleanToArrayOfString() throws Exception {
  }

  @Test
  public void mapLongToArrayOfVarchar() throws Exception {
  }

  @Test
  public void mapFloatToArrayOfChar() throws Exception {
  }

  @Test
  public void mapDoubleToArrayOfByte() throws Exception {
  }

  @Test
  public void mapDateToArrayOfShort() throws Exception {
  }

  @Test
  public void mapTimestampToArrayOfInt() throws Exception {
  }

  @Test
  public void mapBinaryToArrayOfDecimal() throws Exception {
  }

  // todo: add cases related to other types
}
