package com.koverse.example.spark

import org.apache.spark.rdd.RDD
import com.koverse.sdk.data.SimpleRecord
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lower

class WordCounter(
    textFieldName: String,
    tokenizationString: String) extends java.io.Serializable {
  
  def count(inputRecordsRdd: RDD[SimpleRecord]): RDD[SimpleRecord] = {
    
    // for each Record, tokenize the specified text field and count each occurrence
    val wordCountRdd = inputRecordsRdd.flatMap { record => record.get(textFieldName).toString().split(tokenizationString) }
                           .map { token => token.toLowerCase().trim() }
                           .map { token => (token, 1) }
                           .reduceByKey { (a,b) => a + b }

    // wordCountRdd is an RDD[(String, Int)] so a (word,count) tuple.
    // turn each tuple into an output Record with a "word" and "count" fields
    val outputRdd = wordCountRdd.map { case(word, count) => {

      val record = new SimpleRecord()
      record.put("word", word)
      record.put("count", count)
      record
    }}
    
    outputRdd
  }
  
  def count(inputDataFrame: DataFrame): DataFrame = {
    
     // Take the column that contains the text and tokenize and count the words
    val wordDF = inputDataFrame.explode(textFieldName, "word") { (text: String) => text.split(tokenizationString) }
    wordDF.select(lower(col("word")).as("lowerWord"))
          .groupBy("lowerWord")
          .count()
  }
  
}