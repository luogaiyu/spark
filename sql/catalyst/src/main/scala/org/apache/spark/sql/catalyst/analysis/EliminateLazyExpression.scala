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
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.LAZY_EXPRESSION

/**
 * `LazyExpression` is a marker node to trigger lazy analysis in DataFrames. It's useless when
 * entering the analyzer and this rule removes it.
 */
 /**
 EliminateLazyExpression 是 Spark SQL 中分析流程中的一个优化步骤，
 通过消除无用的 LazyExpression 节点来简化逻辑计划。这一过程可以提高后续查询的执行效率和可读性，
 因为逻辑计划将只包含实际有意义的表达式和运算。
 **/
object EliminateLazyExpression extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveExpressionsUpWithPruning(_.containsPattern(LAZY_EXPRESSION)) {
      case l: LazyExpression => l.child
    }
  }
}
