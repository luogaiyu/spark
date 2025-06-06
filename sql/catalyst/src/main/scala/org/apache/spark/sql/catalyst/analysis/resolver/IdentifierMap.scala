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

import java.util.Locale

/**
 * The [[IdentifierMap]] is an implementation of a [[KeyTransformingMap]] that uses SQL/DataFrame
 * identifiers as keys. The implementation is case-insensitive for keys.
 */
 // 在SQL中处理管理标识符的场景，避免由于大小写不一致引起的潜在错误
private class IdentifierMap[V] extends KeyTransformingMap[String, V] {
  override def mapKey(key: String): String = key.toLowerCase(Locale.ROOT)
}
