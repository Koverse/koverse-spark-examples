package com.koverse.example.spark

import com.koverse.sdk.transform.spark.sql.JavaSparkSqlTransform
import com.koverse.sdk.transform.spark.sql.JavaSparkSqlTransformContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.koverse.sdk.Version
import com.koverse.sdk.data.Parameter

import scala.collection.JavaConverters._

class WordCountDataFrameTransform extends JavaSparkSqlTransform {

  private val TEXT_COLUMN_NAME_PARAMETER = "textColumnName"

  override def execute(context: JavaSparkSqlTransformContext): DataFrame = {
    
    // The DataCollections are available as DataFrames as they 
    // have been registered as temporary tables in the SQLContext.
    // Access them by collectionId. Here we assume a single collectionId
    val sqlContext = context.getSqlContext()
    val inputCollectionId = context.getJavaSparkTransformContext().getInputCollectionIds().get(0)
    val inputDataFrame = sqlContext.table(inputCollectionId)
    
    // The columns of the DataFrame are the field names of the Record
    val textColumnName = context.getJavaSparkTransformContext().getParameters().get(TEXT_COLUMN_NAME_PARAMETER)
    
    // Take the column that contains the text and tokenize and count the words
    val wordDF = inputDataFrame.explode(textColumnName, "word")((text: String) => text.split("""['".?!,:;\s]"""))
    wordDF.select(lower(col("word")).as("lowerWord"))
          .groupBy("lowerWord")
          .count()
  }
  
  /**
   * The following provide metadata about the Transform used for registration and display in Koverse
   */
  
  override def getName(): String = "Word Count DataFrame Example"

  override def getParameters(): java.lang.Iterable[Parameter] = {
    
    // This parameter will allow the user to input the field name of their Records which 
    // contains the strings that they want to tokenize and count the words from. By parameterizing
    // this field name, we can run this Transform on different Records in different Collections
    // without changing the code
    val textParameter = new Parameter(TEXT_COLUMN_NAME_PARAMETER, "Text Column Name", Parameter.TYPE_STRING)
    Seq(textParameter).asJava
  }

  override def getTypeId(): String = "wordCountDataFrameExample"

  override def getVersion(): Version = new Version(0, 0, 1)
  
  override def getDescription(): String = "The Word Count DataFrame Example"
}