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

package org.apache.spark.sql.catalyst.analysis.resolver

import java.util.ArrayDeque

import org.apache.spark.sql.catalyst.analysis.{RelationResolution, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.{Expression, PlanExpression}
import org.apache.spark.sql.catalyst.plans.logical.{AnalysisHelper, LogicalPlan}
import org.apache.spark.sql.connector.catalog.CatalogManager

/**
 * The [[MetadataResolver]] performs relation metadata resolution based on the unresolved plan
 * at the start of the analysis phase. Usually it does RPC calls to some table catalog and to table
 * metadata itself.
 *
 * [[RelationsWithResolvedMetadata]] is a map from relation ID to the relations with resolved
 * metadata. It's produced by [[resolve]] and is used later in [[Resolver]] to replace
 * [[UnresolvedRelation]]s.
 *
 * This object is one-shot per SQL query or DataFrame program resolution.
 */
 /**
 通过未解析关系的元数据来填充信息，利用逻辑计划树的遍历有效处理关系解析
 **/
class MetadataResolver(
    override val catalogManager: CatalogManager,
    override val relationResolution: RelationResolution,
    override val extensions: Seq[ResolverExtension] = Seq.empty)
    extends RelationMetadataProvider
    with DelegatesResolutionToExtensions {
  override val relationsWithResolvedMetadata = new RelationsWithResolvedMetadata

  /**
   * [[ProhibitedResolver]] passed as an argument to [[tryDelegateResolutionToExtensions]], because
   * unresolved subtree resolution doesn't make sense during metadata resolution traversal.
   */
  private val prohibitedResolver = new ProhibitedResolver

  /**
   * Resolves the relation metadata for `unresolvedPlan`. Usually this involves several blocking
   * calls for the [[UnresolvedRelation]]s present in that tree. During the `unresolvedPlan`
   * traversal we fill [[relationsWithResolvedMetadata]] with resolved metadata by relation id.
   * This map will be used to resolve the plan in single-pass by the [[Resolver]] using
   * [[getRelationWithResolvedMetadata]]. If the generic metadata resolution using
   * [[RelationResolution]] wasn't successful, we resort to using [[extensions]].
   * Otherwise, we fail with an exception.
   */
  override def resolve(unresolvedPlan: LogicalPlan): Unit = {
    traverseLogicalPlanTree(unresolvedPlan) {
      case unresolvedRelation: UnresolvedRelation =>
        val relationId = relationIdFromUnresolvedRelation(unresolvedRelation)

        if (!relationsWithResolvedMetadata.containsKey(relationId)) {
          val relationWithResolvedMetadata = resolveRelation(unresolvedRelation).orElse {
            // In case the generic metadata resolution returned `None`, we try to check if any
            // of the [[extensions]] matches this `unresolvedRelation`, and resolve it using
            // that extension.
            tryDelegateResolutionToExtension(unresolvedRelation, prohibitedResolver)
          }

          relationWithResolvedMetadata match {
            case Some(relationWithResolvedMetadata) =>
              relationsWithResolvedMetadata.put(
                relationId,
                relationWithResolvedMetadata
              )
            case None =>
          }
        }
      case _ =>
    }
  }

  /**
   * Resolves the metadata for the given unresolved relation and returns a relation with the
   * resolved metadata. This method is blocking.
   */
  private def resolveRelation(unresolvedRelation: UnresolvedRelation): Option[LogicalPlan] =
    AnalysisHelper.allowInvokingTransformsInAnalyzer {
      relationResolution.resolveRelation(
        u = unresolvedRelation
      )
    }

  /**
   * Traverse the logical plan tree from `root` in a pre-order DFS manner and apply `visitor` to
   * each node. This function handles the whole operator tree, its child expression subqueries and
   * inner children (e.g. [[UnresolvedWith]] CTE definitions).
   */
  private def traverseLogicalPlanTree(root: LogicalPlan)(visitor: LogicalPlan => Unit) = {
    val stack = new ArrayDeque[Either[LogicalPlan, Expression]]
    stack.push(Left(root))

    while (!stack.isEmpty) {
      stack.pop() match {
        case Left(logicalPlan) =>
          visitor(logicalPlan)

          for (child <- logicalPlan.children) {
            stack.push(Left(child))
          }
          for (innerChild <- logicalPlan.innerChildren) {
            innerChild match {
              case plan: LogicalPlan =>
                stack.push(Left(plan))
              case _ =>
            }
          }
          for (expression <- logicalPlan.expressions) {
            stack.push(Right(expression))
          }
        case Right(expression) =>
          for (child <- expression.children) {
            stack.push(Right(child))
          }

          expression match {
            case planExpression: PlanExpression[_] =>
              planExpression.plan match {
                case plan: LogicalPlan =>
                  stack.push(Left(plan))
                case _ =>
              }
            case _ =>
          }
      }
    }
  }
}
