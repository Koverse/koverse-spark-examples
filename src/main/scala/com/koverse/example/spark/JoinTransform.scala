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

import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.JavaConverters.seqAsJavaListConverter

class JoinTransform extends JavaSparkTransform {

  private val COLLECTION1_JOIN_FIELD_NAME_PARAMETER = "collection1JoinFieldName"
  private val COLLECTION2_JOIN_FIELD_NAME_PARAMETER = "collection2JoinFieldName"

  /**
   * Koverse calls this method to execute your transform.
   *
   * @param context The context of this spark execution
   * @return The resulting RDD of this transform execution.
   *         It will be applied to the output collection.
   */
  override def execute(context: JavaSparkTransformContext): JavaRDD[SimpleRecord] = {

    // This transform assumes there are two input Data Collection which will be joined
    val inputCollection1Id = context.getInputCollectionIds().get(0)
    val inputCollection2Id = context.getInputCollectionIds().get(1)

    // Get the RDD[SimpleRecord] that represents each input Data Collection
    val inputRecords1Rdd = context.getInputCollectionRdds.get(inputCollection1Id).rdd
    val inputRecords2Rdd = context.getInputCollectionRdds.get(inputCollection2Id).rdd

    // get the field names for each collection which we are going to join on
    val collection1FieldName = context.getParameters().get(COLLECTION1_JOIN_FIELD_NAME_PARAMETER)
    val collection2FieldName = context.getParameters().get(COLLECTION2_JOIN_FIELD_NAME_PARAMETER)

    // Map the RDDs to be an RDD[(key, SimpleRecord)] as is required to join in Spark
    val inputPairs1Rdd = inputRecords1Rdd.map(record => (record.get(collection1FieldName).toString(), record))
    val inputPairs2Rdd = inputRecords2Rdd.map(record => (record.get(collection2FieldName).toString(), record))

    // Perform the join
    val joinedRecordsRdd = inputPairs1Rdd.join(inputPairs2Rdd)

    // Combine the two joined Records into one by putting all of the field/values into the new Record
    val outputRdd = joinedRecordsRdd.map({ case(key, (record1, record2)) => {

      val record = new SimpleRecord()
      record1.entrySet().asScala.foreach(entry => record.put(entry.getKey(), entry.getValue()))
      record2.entrySet().asScala.foreach(entry => record.put(entry.getKey(), entry.getValue()))
      record
    }})

    outputRdd
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
  override def getName(): String = "Join Example"

  /**
   * Get the parameters of this transform.  The returned iterable can
   * be immutable, as it will not be altered.
   *
   * @return The parameters of this transform.
   */
  override def getParameters(): java.lang.Iterable[Parameter] = {

    // This parameter will allow the user to input the field names of their Records
    // from each Collection which to join on
    val collection1JoinParameter = Parameter.newBuilder()
      .parameterName(COLLECTION1_JOIN_FIELD_NAME_PARAMETER)
      .displayName("Collection 1 Join Field Name")
      .`type`(Parameter.TYPE_STRING)
      .required(true)
      .build()
      
    val collection2JoinParameter = Parameter.newBuilder()
      .parameterName(COLLECTION2_JOIN_FIELD_NAME_PARAMETER)
      .displayName("Collection 2 Join Field Name")
      .`type`(Parameter.TYPE_STRING)
      .required(true)
      .build()
  
    Seq(collection1JoinParameter, collection2JoinParameter).asJava
  }

  /**
   * Get the programmatic identifier for this transform.  It must not
   * be an empty string and must contain only alpha numeric characters.
   *
   * @return The programmatic id of this transform.
   */
  override def getTypeId(): String = "joinExample"

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
  override def getDescription(): String = "This is the Scala Join Example Transform"
}
