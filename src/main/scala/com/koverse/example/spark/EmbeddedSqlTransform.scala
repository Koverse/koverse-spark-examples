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

class EmbeddedSqlTransform extends JavaSparkSqlTransform {

  override def execute(context: JavaSparkSqlTransformContext): DataFrame = {

    // The DataCollections are available as DataFrames as they
    // have been registered as temporary tables in the SQLContext.
    // Access them by collectionId. Here we assume a single collectionId
    val sqlContext = context.getSqlContext()
    val inputCollectionId = context.getJavaSparkTransformContext().getInputCollectionIds().get(0)

    // Embed SQL queries
    val filtered = sqlContext.sql(s"SELECT * FROM $inputCollectionId WHERE age > 30")
    
    // Use Spark DataFrame operations
    val grouped = filtered.select("name")
                    .groupBy("name")
                    .count()
    return grouped
  }

 
  override def getName(): String = "Embedded SQL Transform"
  
  override def getParameters(): java.lang.Iterable[Parameter] = Seq[Parameter]().asJava

  override def getTypeId(): String = "embeddedSqlTransformExample"

  override def getVersion(): Version = new Version(0, 0, 1)

  override def getDescription(): String = "Embedded SQL"
}
