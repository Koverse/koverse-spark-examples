/*
 * Copyright 2016 Koverse, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.koverse.example.spark

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest._
import org.junit.runner.RunWith
import org.junit.Assert._
import org.scalatest.junit.JUnitRunner
import com.koverse.sdk.data.SimpleRecord

import scala.collection.JavaConverters._
import org.apache.spark.sql.{ SparkSession}

/**
 * These tests leverage the great work at https://github.com/holdenk/spark-testing-base
 */
@RunWith(classOf[JUnitRunner])
class WordCounterTest extends FunSuite with SharedSparkContext {

  test("RDD test") {
    val inputRecords = List(
        new SimpleRecord(Map[String,Object]("text" -> "these words are to be counted", "id" -> "0").asJava),
        new SimpleRecord(Map[String,Object]("text" -> "more words    that are worth counting", "id" -> "1").asJava))

    val inputRecordsRdd = sc.parallelize(inputRecords)
    val wordCounter = new WordCounter("text", """['".?!,:;\s]+""")
    val outputRecordsRdd = wordCounter.count(inputRecordsRdd)

    assertEquals(outputRecordsRdd.count, 10)
    val outputRecords = outputRecordsRdd.collect()
    val countRecordOption = outputRecords.find { simpleRecord => simpleRecord.get("word").equals("are") }

    assertTrue(countRecordOption.isDefined)
    assertEquals(countRecordOption.get.get("count"), 2)
  }

  case class Message(text: String, id: String)

  test("DataFrame test") {

    val messages = List(
        Message("these words are to be counted", "0"),
        Message("more words that are worth counting", "1"))

    val inputDataFrame = SparkSession.builder().appName("DataFrame Test").getOrCreate().createDataFrame(messages)
    val wordCounter = new WordCounter("text", """['".?!,:;\s]""")
    val outputDataFrame = wordCounter.count(inputDataFrame)

    assertEquals(outputDataFrame.count(), 10)
    val outputRows = outputDataFrame.collect()
    val countRowOption = outputRows.find { row => row.getAs[String]("lowerWord").equals("are") }
    assertTrue(countRowOption.isDefined)
    assertEquals(countRowOption.get.getAs[Long]("count"), 2)

  }
}
