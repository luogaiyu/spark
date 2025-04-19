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

import org.apache.spark.sql.catalyst.expressions.{Cast, Divide, Expression}
import org.apache.spark.sql.types.{DecimalType, DoubleType, NullType, NumericType}

/**
 * Type coercion helper that matches against [[Divide]] expressions in order to type coerce
 * children to [[DoubleType]].
 */
 /**
 对于 Spark SQL 的解析和执行计划生成至关重要，确保了除法表达式在计算前的类型一致性。通过将操作数统一转换为 DoubleType，可以避免因类型不匹配而引发的计算错误，从而提升了系统的健壮性和执行效率。

 **/
object DivisionTypeCoercion {
  def apply(expression: Expression): Expression = expression match {
    // Decimal and Double remain the same
    case d: Divide if d.dataType == DoubleType => d
    case d: Divide if d.dataType.isInstanceOf[DecimalType] => d
    case d @ Divide(left, right, _) if isNumericOrNull(left) && isNumericOrNull(right) =>
      d.copy(left = Cast(left, DoubleType), right = Cast(right, DoubleType))
    case other => other
  }

  private def isNumericOrNull(ex: Expression): Boolean = {
    // We need to handle null types in case a query contains null literals.
    ex.dataType.isInstanceOf[NumericType] || ex.dataType == NullType
  }
}
