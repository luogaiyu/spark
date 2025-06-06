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

import scala.util.{Either, Left, Right}

import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, VariableReference}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.{CompoundBody, LogicalPlan, SetVariable}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{EXECUTE_IMMEDIATE, TreePattern}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.StringType

/**
 * Logical plan representing execute immediate query.
 *
 * @param args parameters of query
 * @param query query string or variable
 * @param targetVariables variables to store the result of the query
 */
 /**
 主要用于处理 SQL 中的 EXECUTE IMMEDIATE 查询。这个类的目的是解析和实现执行即时 SQL 查询的逻辑，
 尤其是动态地执行一个字符串形式的 SQL 语句，并将结果存储在目标变量中。下面是对这段代码的详细解释。
 **/
case class ExecuteImmediateQuery(
    args: Seq[Expression],
    query: Either[String, UnresolvedAttribute],
    targetVariables: Seq[UnresolvedAttribute])
  extends UnresolvedLeafNode {
  final override val nodePatterns: Seq[TreePattern] = Seq(EXECUTE_IMMEDIATE)
}

/**
 * This rule substitutes execute immediate query node with fully analyzed
 * plan that is passed as string literal or session parameter.
 */
class SubstituteExecuteImmediate(
    val catalogManager: CatalogManager,
    resolveChild: LogicalPlan => LogicalPlan,
    checkAnalysis: LogicalPlan => Unit)
  extends Rule[LogicalPlan] with ColumnResolutionHelper {

  def resolveVariable(e: Expression): Expression = {

    /**
     * We know that the expression is either UnresolvedAttribute, Alias or Parameter, as passed from
     * the parser. If it is an UnresolvedAttribute, we look it up in the catalog and return it. If
     * it is an Alias, we resolve the child and return an Alias with the same name. If it is
     * a Parameter, we leave it as is because the parameter belongs to another parameterized
     * query and should be resolved later.
     */
    e match {
      case u: UnresolvedAttribute =>
        getVariableReference(u, u.nameParts)
      case a: Alias =>
        Alias(resolveVariable(a.child), a.name)()
      case p: Parameter => p
      case other =>
        throw QueryCompilationErrors.unsupportedParameterExpression(other)
    }
  }

  def resolveArguments(expressions: Seq[Expression]): Seq[Expression] = {
    expressions.map { exp =>
      if (exp.resolved) {
        exp
      } else {
        resolveVariable(exp)
      }
    }
  }

  def extractQueryString(either: Either[String, UnresolvedAttribute]): String = {
    either match {
      case Left(v) => v
      case Right(u) =>
        val varReference = getVariableReference(u, u.nameParts)

        if (!varReference.dataType.sameType(StringType)) {
          throw QueryCompilationErrors.invalidExecuteImmediateVariableType(varReference.dataType)
        }

        // Call eval with null value passed instead of a row.
        // This is ok as this is variable and invoking eval should
        // be independent of row value.
        val varReferenceValue = varReference.eval(null)

        if (varReferenceValue == null) {
          throw QueryCompilationErrors.nullSQLStringExecuteImmediate(u.name)
        }

        varReferenceValue.toString
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.resolveOperatorsWithPruning(_.containsPattern(EXECUTE_IMMEDIATE), ruleId) {
      case e @ ExecuteImmediateQuery(expressions, _, _) if expressions.exists(!_.resolved) =>
        e.copy(args = resolveArguments(expressions))

      case ExecuteImmediateQuery(expressions, query, targetVariables)
        if expressions.forall(_.resolved) =>

        val queryString = extractQueryString(query)
        val plan = parseStatement(queryString, targetVariables)

        val posNodes = plan.collect { case p: LogicalPlan =>
          p.expressions.flatMap(_.collect { case n: PosParameter => n })
        }.flatten
        val namedNodes = plan.collect { case p: LogicalPlan =>
          p.expressions.flatMap(_.collect { case n: NamedParameter => n })
        }.flatten

        val queryPlan = if (expressions.isEmpty || (posNodes.isEmpty && namedNodes.isEmpty)) {
          plan
        } else if (posNodes.nonEmpty && namedNodes.nonEmpty) {
          throw QueryCompilationErrors.invalidQueryMixedQueryParameters()
        } else {
          if (posNodes.nonEmpty) {
            PosParameterizedQuery(plan, expressions)
          } else {
            val aliases = expressions.collect {
              case e: Alias => e
              case u: VariableReference => Alias(u, u.identifier.name())()
            }

            if (aliases.size != expressions.size) {
              val nonAliases = expressions.filter(attr =>
                !attr.isInstanceOf[Alias] && !attr.isInstanceOf[VariableReference])

              throw QueryCompilationErrors.invalidQueryAllParametersMustBeNamed(nonAliases)
            }

            NameParameterizedQuery(
              plan,
              aliases.map(_.name),
              // We need to resolve arguments before Resolution batch to make sure
              // that some rule does not accidentally resolve our parameters.
              // We do not want this as they can resolve some unsupported parameters.
              aliases)
          }
        }

        // Fully analyze the generated plan. AnalysisContext.withExecuteImmediateContext makes sure
        // that SQL scripting local variables will not be accessed from the plan.
        val finalPlan = AnalysisContext.withExecuteImmediateContext {
          resolveChild(queryPlan)
        }
        checkAnalysis(finalPlan)

        if (targetVariables.nonEmpty) {
          SetVariable(targetVariables, finalPlan)
        } else { finalPlan }
    }

  private def parseStatement(
      queryString: String,
      targetVariables: Seq[Expression]): LogicalPlan = {
    // If targetVariables is defined, statement needs to be a query.
    // Otherwise, it can be anything.
    val plan = if (targetVariables.nonEmpty) {
      try {
        catalogManager.v1SessionCatalog.parser.parseQuery(queryString)
      } catch {
        case e: ParseException =>
          // Since we do not have a way of telling that parseQuery failed because of
          // actual parsing error or because statement was passed where query was expected,
          // we need to make sure that parsePlan wouldn't throw
          catalogManager.v1SessionCatalog.parser.parsePlan(queryString)

          // Plan was successfully parsed, but query wasn't - throw.
          throw QueryCompilationErrors.invalidStatementForExecuteInto(queryString)
      }
    } else {
      catalogManager.v1SessionCatalog.parser.parsePlan(queryString)
    }

    if (plan.isInstanceOf[CompoundBody]) {
      throw QueryCompilationErrors.sqlScriptInExecuteImmediate(queryString)
    }

    // do not allow nested execute immediate
    if (plan.containsPattern(EXECUTE_IMMEDIATE)) {
      throw QueryCompilationErrors.nestedExecuteImmediate(queryString)
    }

    plan
  }

  private def getVariableReference(expr: Expression, nameParts: Seq[String]): VariableReference = {
    lookupVariable(nameParts) match {
      case Some(variable) => variable
      case _ =>
        throw QueryCompilationErrors
          .unresolvedVariableError(
            nameParts,
            Seq(CatalogManager.SYSTEM_CATALOG_NAME, CatalogManager.SESSION_NAMESPACE),
            expr.origin)
    }
  }
}
