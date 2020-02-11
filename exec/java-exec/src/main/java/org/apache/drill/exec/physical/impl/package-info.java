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
 * Congratulations! You've reached the depths of query batch execution, the execution DAG created by
 * {@link org.apache.drill.exec.physical.impl.ImplCreator} consists of different operators you can find in
 * the underlying packages. In turn, these operators know how to generate optimized code to process specific
 * vector's data in record batches.
 */
package org.apache.drill.exec.physical.impl;