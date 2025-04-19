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

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, DistributedSequenceID}
import org.apache.spark.sql.catalyst.plans.logical.{AttachDistributedSequence, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.DISTRIBUTED_SEQUENCE_ID
import org.apache.spark.sql.types.LongType

/**
提供了一种处理逻辑计划中分布式序列的机制，确保在执行中能正确生成和附加序列 ID，提高了对大规模数据集的处理能力。这种设计增强了 Spark 在分布式环境中的灵活性，确保生成的序列能够与分布式数据的上下文相匹配。
**/
/**
 * Extracts [[DistributedSequenceID]] in logical plans, and replace it to
 * [[AttachDistributedSequence]] because this expressions requires a shuffle
 * to generate a sequence that needs the context of the whole data, e.g.,
 * [[org.apache.spark.rdd.RDD.zipWithIndex]].
 */
object ExtractDistributedSequenceID extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsUpWithPruning(_.containsPattern(DISTRIBUTED_SEQUENCE_ID)) {
      case plan: LogicalPlan if plan.resolved &&
          plan.expressions.exists(_.exists(_.isInstanceOf[DistributedSequenceID])) =>
        val attr = AttributeReference("distributed_sequence_id", LongType, nullable = false)()
        val newPlan = plan.withNewChildren(plan.children.map(AttachDistributedSequence(attr, _)))
          .transformExpressions { case _: DistributedSequenceID => attr }
        Project(plan.output, newPlan)
    }
  }
}
