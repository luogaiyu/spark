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

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.errors.QueryErrorsBase

/**
 * Base class for [[TreeNode]] resolvers. All resolvers should extend this class with
 * specific [[UnresolvedNode]] and [[ResolvedNode]] types.
 */
 // 用于定义 未解析和已解析节点之间的解析关系
trait TreeNodeResolver[UnresolvedNode <: TreeNode[_], ResolvedNode <: TreeNode[_]]
    extends SQLConfHelper
    with QueryErrorsBase {
  def resolve(unresolvedNode: UnresolvedNode): ResolvedNode
}
