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
/**
 * Logical operators in a query graph are contained in the package:
 *
 * <table>
 *
 * <tr>
 * <th>Operator</th>
 * <th>Description</th>
 * </tr>
 *
 * <tr>
 * <td>{@link org.apache.drill.common.logical.data.Scan}</td>
 * <td>The Scan operator outputs a stream of records.</td>
 * <tr/>
 *
 * <tr>
 * <td>{@link org.apache.drill.common.logical.data.Values}</td>
 * <td>The Values operator returns a constant result.</td>
 * </tr>
 *
 * <tr>
 * <td>{@link org.apache.drill.common.logical.data.Join}</td>
 * <td>
 * The Join operator joins two inputs based on one or more join conditions. The output of this operator is the combination of the two inputs.
 * This is done by providing a combination record for each set of input records that matches all provided join conditions.
 * </td>
 * </tr>
 *
 * <tr>
 * <td>{@link org.apache.drill.common.logical.data.Union}</td>
 * <td>The Union operator combines two or more data inputs into a single stream.</td>
 * </tr>
 *
 * <tr>
 * <td>{@link org.apache.drill.common.logical.data.Store}</td>
 * <td>The Store operator stores the stream output to a storage engine.</td>
 * </tr>
 *
 * <tr>
 * <td>{@link org.apache.drill.common.logical.data.Project}</td>
 * <td>
 * The Project operator returns a particular fields subset of the incoming stream of data corresponding to the expressions provided.
 * </td>
 * </tr>
 *
 *
 * <tr>
 * <td>{@link org.apache.drill.common.logical.data.Order}</td>
 * <td>
 * The Order operator orders an input stream by one or more ordering expressions.
 * </td>
 * </tr>
 * // TODO: finish doc based on https://docs.google.com/document/d/1QTL8warUYS2KjldQrGUse7zp8eA72VKtLOHwfXy6c7I/edit#heading=h.wefr69kgi76d
 *
 *
 * </table>
 */
package org.apache.drill.common.logical.data;