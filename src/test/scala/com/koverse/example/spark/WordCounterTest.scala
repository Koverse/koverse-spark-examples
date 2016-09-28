package com.koverse.example.spark

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.junit.Assert._
import org.scalatest.junit.JUnitRunner
import com.koverse.sdk.data.SimpleRecord
import scala.collection.JavaConverters._
import com.holdenkarau.spark.testing.DataFrameSuiteBase

/**
 * These tests leverage the great work at https://github.com/holdenk/spark-testing-base
 */
@RunWith(classOf[JUnitRunner])
class WordCounterTest extends DataFrameSuiteBase{
  
  test("RDD test") {
    val inputRecords = List(
        new SimpleRecord(Map[String,Object]("text" -> "these words are to be counted", "id" -> "0").asJava),
        new SimpleRecord(Map[String,Object]("text" -> "more words that are worth counting", "id" -> "1").asJava))
        
    val inputRecordsRdd = sc.parallelize(inputRecords)
    val wordCounter = new WordCounter("text", """['".?!,:;\s]""")
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
        
    val inputDataFrame = sqlContext.createDataFrame(messages)
    val wordCounter = new WordCounter("text", """['".?!,:;\s]""")
    val outputDataFrame = wordCounter.count(inputDataFrame)
    
    assertEquals(outputDataFrame.count(), 10)
    val outputRows = outputDataFrame.collect()
    val countRowOption = outputRows.find { row => row.getAs[String]("lowerWord").equals("are") }
    assertTrue(countRowOption.isDefined)
    assertEquals(countRowOption.get.getAs[Long]("count"), 2)
    
  }
}