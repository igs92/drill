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
package org.apache.drill.exec.physical.impl.join;

import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTmpIntersectAndJoin extends ClusterTest {

  @BeforeClass
  public static void setUp() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher)
        .configProperty(PlannerSettings.MERGEJOIN.getOptionName(), false)
    );
  }

  @Test
  public void testHashJoin() throws Exception {
    queryBuilder().sql("SELECT l.full_name " +
        "FROM cp.`employee.json` l " +
        "INNER JOIN cp.`employee.json` r " +
        "ON l.hire_date = r.hire_date AND l.employee_id <> r.employee_id")
        .run();
  }

  @Test
  public void testIntersectSyntax() throws Exception {
    String sql = "SELECT full_name " +
        "FROM cp.`employee.json` " +
        "WHERE employee_id > 99 AND employee_id < 200" +
        "INTERSECT " +
        "SELECT full_name " +
        "FROM cp.`employee.json` " +
        "WHERE gender='M'";
    queryBuilder()
        .sql(sql)
        .run();
  }

  @Test
  public void testExplainLogicalIntersectSyntax() throws Exception {
    String sql = "EXPLAIN PLAN WITHOUT IMPLEMENTATION FOR " +
        "SELECT full_name " +
        "FROM cp.`employee.json` " +
        "WHERE employee_id > 99 AND employee_id < 200" +
        "INTERSECT " +
        "SELECT full_name " +
        "FROM cp.`employee.json` " +
        "WHERE gender='M'";
    queryBuilder()
        .sql(sql)
        .run();
  }

  @Test
  public void testExplainLogicalUnionSyntax() throws Exception {
    String sql = "EXPLAIN PLAN WITHOUT IMPLEMENTATION FOR " +
        "SELECT full_name " +
        "FROM cp.`employee.json` " +
        "WHERE employee_id > 99 AND employee_id < 200" +
        "UNION " +
        "SELECT full_name " +
        "FROM cp.`employee.json` " +
        "WHERE gender='M'";
    queryBuilder()
        .sql(sql)
        .run();
  }

  @Test
  public void testSimpleHashAgg() throws Exception {
    String sql = "SELECT count(employee_id) employees, position_title " +
        "FROM cp.`employee.json` " +
        "GROUP BY position_title";
    queryBuilder()
        .sql(sql)
        .run();
  }
}
