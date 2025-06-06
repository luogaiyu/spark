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

import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, GetDateField}
import org.apache.spark.sql.types.{AnyTimestampTypeExpression, DateType}

/**
 * ANSI type coercion helper that matches against [[GetDateField]] expressions in order to type
 * coerce children to [[DateType]], if necessary.
 */
 // 处理 GetDataField 表达式的类型转化逻辑，GetDateField 的子表达式能被转换为 DateType。
object AnsiGetDateFieldOperationsTypeCoercion {
  def apply(expression: Expression): Expression = expression match {
    case g: GetDateField if AnyTimestampTypeExpression.unapply(g.child) =>
      g.withNewChildren(Seq(Cast(g.child, DateType)))

    case other => other
  }
}
