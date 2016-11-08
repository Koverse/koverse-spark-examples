package com.koverse.example.spark

import junit.framework.TestCase
import org.junit.Assert._
import com.koverse.sdk.data.SimpleRecord
import scala.collection.JavaConverters._
import com.koverse.sdk.test.spark.SparkTransformTestRunner

class WordCountTransformTest extends TestCase {
  
  def test() = {
    val inputRecords = List(
        new SimpleRecord(Map[String,Object]("text" -> "these words are to be counted", "id" -> "0").asJava),
        new SimpleRecord(Map[String,Object]("text" -> "more words that are worth counting", "id" -> "1").asJava)).asJava
    val collectionId = "testCollection"
    
    val outputRecords = SparkTransformTestRunner.runTest(
        classOf[WordCountTransform], 
        Map[String,String]("textFieldName" -> "text").asJava,
        Map(collectionId -> inputRecords).asJava, 
        "outputCollectionId")
    
    // there should be 10 unique words that were counted
    assertEquals(outputRecords.size(), 10)
    
    // the count for word "are" should be 2
    val optionalRecord = outputRecords.asScala.find { record => record.get("word").equals("are") }
    assertTrue(optionalRecord.isDefined)
    assertEquals(optionalRecord.get.get("count"), 2)
  }
}