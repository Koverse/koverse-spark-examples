package com.koverse.example.spark

import java.lang


import com.koverse.com.google.common.collect.Lists
import com.koverse.sdk.Version
import com.koverse.sdk.data.Parameter
import com.koverse.sdk.transform.spark.sql.{JavaSparkDataFrameTransform, JavaSparkDataFrameTransformContext}
import org.apache.spark.sql.DataFrame

class StoreCaseClassMapDataFrameTransform extends JavaSparkDataFrameTransform {

  override def execute(sqlTransformContext: JavaSparkDataFrameTransformContext): DataFrame = {
    val sqlContext = sqlTransformContext.getSqlContext()
    import sqlContext.implicits._

    val df = sqlContext.sparkContext.parallelize(Seq(
      MapTest(1, Map("a"->"b", "b"->"c")),
      MapTest(2, Map("z"->"y", "y"->"x"))), 1).toDF()

    return df

  }

  override def getName: String = "Store Case Class Map DataFrame Transform"

  override def getTypeId: String = "StoreCaseClassMapDataFrameTransform"

  override def getVersion: Version = new Version(0, 0, 1)

  override def getDescription: String = "Store Case Class Map DataFrame Transform"

  override def getParameters: lang.Iterable[Parameter] = Lists.newArrayList[Parameter]()
}

case class MapTest(id: Int, properties: Map[String, String])
