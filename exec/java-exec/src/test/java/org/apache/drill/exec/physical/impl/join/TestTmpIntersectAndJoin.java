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

import java.nio.file.Paths;

public class TestTmpIntersectAndJoin extends ClusterTest {

  @BeforeClass
  public static void setUp() throws Exception {
//    dirTestWatcher.copyFileToRoot(Paths.get("tmp", "studs.json"));
//    dirTestWatcher.copyFileToRoot(Paths.get("tmp", "deps.json"));
    startCluster(ClusterFixture.builder(dirTestWatcher)
        .sessionOption(PlannerSettings.MERGEJOIN.getOptionName(), false)
        .sessionOption(PlannerSettings.STREAMAGG.getOptionName(), false)
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

  @Test
  public void outStudentsAndDeps() throws Exception {
    String sql = "SELECT s.full_name, d.dept_name " +
        "FROM cp.`tmp/studs.json` s " +
        "INNER JOIN cp.`tmp/depts.json` d " +
        "ON s.dept_id = d.id";

    if (false) {
      outPlan(sql);
    }

    queryBuilder()
        .sql(sql)
        .print();
  }

  @Test
  public void countStudentsByDepsNoJoin() throws Exception {
    String sql = "SELECT s.dept_id, count(s.dept_id) " +
        "FROM cp.`tmp/studs.json` s " +
        "GROUP BY s.dept_id";
    if (false) {
      outPlan(sql);
    }

    queryBuilder()
        .sql(sql)
        .print();
  }



  @Test
  public void countStudentsByDepsWithJoin() throws Exception {
    String sql = "SELECT d.dept_name, count(s.dept_id) " +
        "FROM cp.`tmp/depts.json` d " +
        "INNER JOIN cp.`tmp/studs.json` s " +
        "ON s.dept_id = d.id " +
        "GROUP BY d.dept_name";

    if (true) {
      outPlan(sql);
    }

    queryBuilder()
        .sql(sql)
        .print();
  }

  private void outPlan(String sql) throws Exception {
    String plan = queryBuilder()
        .sql(sql)
        .explainText();
    System.out.println(plan);
  }
}
