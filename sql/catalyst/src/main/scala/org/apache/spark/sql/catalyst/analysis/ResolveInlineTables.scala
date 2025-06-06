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

import org.apache.spark.sql.catalyst.EvaluateUnresolvedInlineTable
import org.apache.spark.sql.catalyst.expressions.EvalHelper
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.AlwaysProcess
// 处理内连表解析的重要组件，内连
/**
 * An analyzer rule that replaces [[UnresolvedInlineTable]] with [[ResolvedInlineTable]].
 */
object ResolveInlineTables extends Rule[LogicalPlan] with EvalHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsWithPruning(AlwaysProcess.fn, ruleId) {
      case table: UnresolvedInlineTable if table.expressionsResolved =>
        EvaluateUnresolvedInlineTable.evaluateUnresolvedInlineTable(table)
    }
  }
}
