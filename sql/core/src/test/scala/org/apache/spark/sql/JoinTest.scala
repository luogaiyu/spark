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

package org.apache.spark.sql

import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.Row

class JoinTest extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("inner join - basic") {
    val df1 = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "value")
    val df2 = Seq((1, "A"), (2, "B"), (4, "D")).toDF("id", "value")

    checkAnswer(
      df1.join(df2, "id"),
      Row(1, "a", "A") :: Row(2, "b", "B") :: Nil)
  }

  test("left outer join") {
    val df1 = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "value")
    val df2 = Seq((1, "A"), (2, "B"), (4, "D")).toDF("id", "value")

    checkAnswer(
      df1.join(df2, Seq("id"), "left"),
      Row(1, "a", "A") :: Row(2, "b", "B") :: Row(3, "c", null) :: Nil)
  }

  test("right outer join") {
    val df1 = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "value")
    val df2 = Seq((1, "A"), (2, "B"), (4, "D")).toDF("id", "value")

    checkAnswer(
      df1.join(df2, Seq("id"), "right"),
      Row(1, "a", "A") :: Row(2, "b", "B") :: Row(4, null, "D") :: Nil)
  }

  test("full outer join") {
    val df1 = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "value")
    val df2 = Seq((1, "A"), (2, "B"), (4, "D")).toDF("id", "value")

    checkAnswer(
      df1.join(df2, Seq("id"), "outer"),
      Row(1, "a", "A") :: Row(2, "b", "B") :: Row(3, "c", null) :: Row(4, null, "D") :: Nil)
  }

  test("left semi join") {
    val df1 = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "value")
    val df2 = Seq((1, "A"), (2, "B"), (4, "D")).toDF("id", "value")

    checkAnswer(
      df1.join(df2, Seq("id"), "left_semi"),
      Row(1, "a") :: Row(2, "b") :: Nil)
  }

  test("left anti join") {
    val df1 = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "value")
    val df2 = Seq((1, "A"), (2, "B"), (4, "D")).toDF("id", "value")

    checkAnswer(
      df1.join(df2, Seq("id"), "left_anti"),
      Row(3, "c") :: Nil)
  }

  test("cross join") {
    val df1 = Seq((1, "a"), (2, "b")).toDF("id", "value")
    val df2 = Seq((1, "A"), (2, "B")).toDF("id", "value")

    checkAnswer(
      df1.crossJoin(df2),
      Row(1, "a", 1, "A") :: Row(1, "a", 2, "B") ::
      Row(2, "b", 1, "A") :: Row(2, "b", 2, "B") :: Nil)
  }

  test("join with multiple conditions") {
    val df1 = Seq((1, "a", 100), (2, "b", 200), (3, "c", 300)).toDF("id", "value", "amount")
    val df2 = Seq((1, "A", 100), (2, "B", 200), (4, "D", 400)).toDF("id", "value", "amount")

    checkAnswer(
      df1.join(df2, $"df1.id" === $"df2.id" && $"df1.amount" === $"df2.amount"),
      Row(1, "a", 100, 1, "A", 100) :: Row(2, "b", 200, 2, "B", 200) :: Nil)
  }

  test("join with null values") {
    val df1 = Seq((1, "a"), (2, null), (3, "c")).toDF("id", "value")
    val df2 = Seq((1, "A"), (2, "B"), (4, "D")).toDF("id", "value")

    checkAnswer(
      df1.join(df2, Seq("id"), "left"),
      Row(1, "a", "A") :: Row(2, null, "B") :: Row(3, "c", null) :: Nil)
  }

  test("self join") {
    val df = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "value")
    
    checkAnswer(
      df.as("left").join(df.as("right"), $"left.id" === $"right.id"),
      Row(1, "a", 1, "a") :: Row(2, "b", 2, "b") :: Row(3, "c", 3, "c") :: Nil)
  }
} 