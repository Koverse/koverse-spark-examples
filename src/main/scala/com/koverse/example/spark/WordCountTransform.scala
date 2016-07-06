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

import com.koverse.sdk.transform.spark.JavaSparkTransform
import com.koverse.sdk.transform.spark.JavaSparkTransformContext
import com.koverse.sdk.data.SimpleRecord
import org.apache.spark.api.java.JavaRDD
import com.koverse.sdk.Version
import com.koverse.sdk.data.Parameter

import scala.collection.JavaConverters._

class WordCountTransform extends JavaSparkTransform {
  
  private val TEXT_FIELD_NAME_PARAMETER = "textFieldName"
    
  override def execute(context: JavaSparkTransformContext): JavaRDD[SimpleRecord] = {
    
    // This transform assumes there is a single input Data Collection
    val inputCollectionId = context.getInputCollectionIds().get(0)
    
    // Get the RDD[SimpleRecord] that represents the input Data Collection
    val inputRecordsRdd = context.getInputCollectionRdds.get(inputCollectionId).rdd
    
    // for each Record, tokenize the specified text field and count each occurence
    val fieldName = context.getParameters().get(TEXT_FIELD_NAME_PARAMETER)
    val wordCountRdd = inputRecordsRdd.flatMap(record => record.get(fieldName).toString().split("""['".?!,:;\s]"""))
                           .map(token => token.toLowerCase().trim())
                           .map(token => (token, 1))
                           .reduceByKey((a,b) => a + b)
                           
    // wordCountRdd is an RDD[(String, Int)] so a (word,count) tuple. 
    // turn each tuple into an output Record with a "word" and "count" fields
    val outputRdd = wordCountRdd.map({ case(word, count) => { 
     
      val record = new SimpleRecord()
      record.put("word", word)
      record.put("count", count)
      record
    }})
        
    outputRdd.toJavaRDD
  }
  
  /**
   * The following provide metadata about the Transform used for registration and display in Koverse
   */
  
  override def getName(): String = "Word Count Example"

  override def getParameters(): java.lang.Iterable[Parameter] = {
    
    // This parameter will allow the user to input the field name of their Records which 
    // contains the strings that they want to tokenize and count the words from. By parameterizing
    // this field name, we can run this Transform on different Records in different Collections
    // without changing the code
    val textParameter = new Parameter(TEXT_FIELD_NAME_PARAMETER, "Text Field Name", Parameter.TYPE_STRING)
    Seq(textParameter).asJava
  }

  override def getTypeId(): String = "wordCountExample"

  override def getVersion(): Version = new Version(0, 0, 1)
  
  override def getDescription(): String = "This is the Word Count Example"
}