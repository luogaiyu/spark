/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
/**
总体来看，NamedRelation 提供了一个简单的机制来定义带有明确名称的 SQL 关系，并为管理模式解析提供了灵活性。
它在逻辑计划中扮演着关键角色，通过描述关系和模式的属性来帮助 Spark 分析和优化 SQL 查询。
**/
trait NamedRelation extends LogicalPlan {
  def name: String

  // When false, the schema of input data must match the schema of this relation, during write.
  def skipSchemaResolution: Boolean = false
}
