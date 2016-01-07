package com.koverse.example.spark

import com.koverse.sdk.transform.spark.JavaSparkTransform
import com.koverse.sdk.transform.spark.JavaSparkTransformContext
import com.koverse.sdk.data.SimpleRecord
import org.apache.spark.api.java.JavaRDD
import com.koverse.sdk.Version
import com.koverse.sdk.data.Parameter

import scala.collection.JavaConverters._

class JoinTransform extends JavaSparkTransform {

  private val COLLECTION1_JOIN_FIELD_NAME_PARAMETER = "collection1JoinFieldName"
  private val COLLECTION2_JOIN_FIELD_NAME_PARAMETER = "collection2JoinFieldName"
    
  override def execute(context: JavaSparkTransformContext): JavaRDD[SimpleRecord] = {
    
    // This transform assumes there are two input Data Collection which will be joined
    val inputCollection1Id = context.getInputCollectionIds().get(0)
    val inputCollection2Id = context.getInputCollectionIds().get(1)
    
    // Get the RDD[SimpleRecord] that represents each input Data Collection
    val inputRecords1Rdd = context.getInputCollectionRDDs.get(inputCollection1Id).rdd
    val inputRecords2Rdd = context.getInputCollectionRDDs.get(inputCollection2Id).rdd

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
  
  /**
   * The following provide metadata about the Transform used for registration and display in Koverse
   */
  
  override def getName(): String = "Join Example"

  override def getParameters(): java.lang.Iterable[Parameter] = {
    
    // This parameter will allow the user to input the field names of their Records 
    // from each Collection which to join on
    val collection1JoinParameter = new Parameter(COLLECTION1_JOIN_FIELD_NAME_PARAMETER, "Collection 1 Join Field Name", Parameter.TYPE_STRING)
    val collection2JoinParameter = new Parameter(COLLECTION2_JOIN_FIELD_NAME_PARAMETER, "Collection 2 Join Field Name", Parameter.TYPE_STRING)

    Seq(collection1JoinParameter, collection2JoinParameter).asJava
  }

  override def getTypeId(): String = "joinExample"

  override def getVersion(): Version = new Version(0, 0, 1)
}