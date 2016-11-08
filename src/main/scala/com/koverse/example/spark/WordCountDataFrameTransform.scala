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

import com.koverse.sdk.transform.spark.sql.JavaSparkSqlTransform
import com.koverse.sdk.transform.spark.sql.JavaSparkSqlTransformContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lower

import com.koverse.sdk.Version
import com.koverse.sdk.data.Parameter

import scala.collection.JavaConverters.seqAsJavaListConverter

class WordCountDataFrameTransform extends JavaSparkSqlTransform {

  private val TEXT_COLUMN_NAME_PARAMETER = "textColumnName"

  /**
   * Koverse calls this method to execute your transform.
   *
   * @param context The context of this spark execution
   * @return The resulting RDD of this transform execution.
   *         It will be applied to the output collection.
   */
  override def execute(context: JavaSparkSqlTransformContext): DataFrame = {

    // The DataCollections are available as DataFrames as they
    // have been registered as temporary tables in the SQLContext.
    // Access them by collectionId. Here we assume a single collectionId
    val sqlContext = context.getSqlContext()
    val inputCollectionId = context.getJavaSparkTransformContext().getInputCollectionIds().get(0)
    val inputDataFrame = sqlContext.table(inputCollectionId)

    // The columns of the DataFrame are the field names of the Record
    val textColumnName = context.getJavaSparkTransformContext().getParameters().get(TEXT_COLUMN_NAME_PARAMETER)

   // Create the WordCounter which will perform the logic of our Transform
    val wordCounter = new WordCounter(textColumnName, """['".?!,:;\s]""")
    wordCounter.count(inputDataFrame)
  }

  /*
   * The following provide metadata about the Transform used for registration
   * and display in Koverse.
   */

  /**
   * Get the name of this transform. It must not be an empty string.
   *
   * @return The name of this transform.
   */
  override def getName(): String = "Word Count DataFrame Example"

  /**
   * Get the parameters of this transform.  The returned iterable can
   * be immutable, as it will not be altered.
   *
   * @return The parameters of this transform.
   */
  override def getParameters(): java.lang.Iterable[Parameter] = {

    // This parameter will allow the user to input the field name of their Records which
    // contains the strings that they want to tokenize and count the words from. By parameterizing
    // this field name, we can run this Transform on different Records in different Collections
    // without changing the code
    val textParameter = Parameter.newBuilder()
      .parameterName(TEXT_COLUMN_NAME_PARAMETER)
      .displayName("Text Column Name")
      .`type`(Parameter.TYPE_STRING)
      .required(true)
      .build()
      
    Seq(textParameter).asJava
  }

  /**
   * Get the programmatic identifier for this transform.  It must not
   * be an empty string and must contain only alpha numeric characters.
   *
   * @return The programmatic id of this transform.
   */
  override def getTypeId(): String = "wordCountDataFrameExample"

  /**
   * Get the version of this transform.
   *
   * @return The version of this transform.
   */
  override def getVersion(): Version = new Version(0, 0, 1)

  /**
   * Get the description of this transform.
   *
   * @return The the description of this transform.
   */
  override def getDescription(): String = "The Word Count DataFrame Example"
}
