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

import java.{lang, sql}
import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.koverse.com.google.common.collect.Lists
import com.koverse.sdk.Version
import com.koverse.sdk.data.Parameter
import com.koverse.sdk.transform.spark.sql.{JavaSparkSqlTransform, JavaSparkSqlTransformContext, KoverseSparkSql}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._

import scala.collection.mutable
//import org.apache.spark.unsafe.types.CalendarInterval

import scala.util.Random

class StoreVariousDataTypesTransform extends JavaSparkSqlTransform {

  override def execute(context: JavaSparkSqlTransformContext): DataFrame = {

    context.getJavaSparkTransformContext.getJavaSparkContext.setLogLevel("DEBUG")


    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val date = formatter.parse("2018-11-07")
    val sqlDate = new sql.Date(date.getTime)
    val timestamp = new Timestamp(System.currentTimeMillis())
    //      val exampleShort:Short = 1
    val binaryType = Array(45,21,4,5,6).map(_.toByte)
    //      val calendarInterval: CalendarInterval = new CalendarInterval(1, 123000L)
//    val mapType:mutable.HashMap[String, Double] = mutable.HashMap("Koverse" -> 2.8 )
    //Java Map
    val mapType:java.util.Map[String, Double] = new java.util.HashMap[String, Double]
    mapType.put("Koverse",2.8 )
    val intArrayType = Array(43,21,4,5,6)
    val nestedArray:Array[Array[Double]] = Array(Array(1.0))

    //Types Commented out do not work in Koverse
    val variousTypesRow = Row(
      "Koverse",//String
      binaryType,//Binary
      true,//Boolean
      //        calendarInterval,//CalendarIntervalType
      sqlDate,//DateType
      //        null, //NullType
      timestamp,//TimestampType
      intArrayType,//ArrayType
//      mapType,//MapType
      nestedArray,//Nested ArrayType
      //        data.get(2).asInstanceOf[String].getBytes(),//ByteType
      Random.nextDouble(),//DoubleType
      //        data.get(2).asInstanceOf[String].getBytes()(0),
      Random.nextInt(),//IntegerType
      //        Random.nextFloat(),//FloatType
      Random.nextLong(),//LongType
      //        exampleShort,//ShortType
      BigDecimal.double2bigDecimal(Random.nextDouble()))//DecimalType

    val variousTypes = context.getSqlContext.sparkContext.parallelize(Seq(variousTypesRow)).toJavaRDD()

    val outputSchema: StructType = StructType(Seq(
      //DataTypes
      StructField("StringType", StringType, true),
      StructField("BinaryType", BinaryType, true),
      StructField("BooleanType", BooleanType, true),
      //      StructField("CalendarIntervalType", CalendarIntervalType, true),
      StructField("DateType", DateType, true),
      //      StructField("NullType", NullType, true),
      //      StructField("StructType",  StructType, true),
      StructField("TimestampType", TimestampType, true),
      //      StructField("UserDefinedType", UserDefinedType, true), VectorUDT
      StructField("ArrayType", ArrayType(IntegerType, true), true),
//      StructField("MapType", DataTypes.createMapType(StringType, DoubleType), true),
      StructField("NestedArrayType", ArrayType(ArrayType(DoubleType, true)), true),
      //Numeric Types
      StructField("DoubleType", DoubleType, true),
      //      StructField("ByteType", ByteType, true),
      StructField("IntegerType", IntegerType, true),
      //      StructField("FloatType", FloatType, true),
      StructField("LongType", LongType, true),
      //      StructField("ShortType", ShortType, true),
      //Decimal Type
      StructField("DecimalType", DecimalType(32,16), true)
      //Object Types
//      StructField("ObjectType", ObjectType, true)
      //      StructField("Metadata", Metadata, true),
      //      StructField("MetadataBuilder", MetadataBuilder, true),
      //      StructField("PrecisionInfo", PrecisionInfo, true),
      //      StructField("StructField", StructField, true),
      //      StructField("AnyDataType", AnyDataType, true)

    ))
    val outputModelDataframe = KoverseSparkSql.createDataFrame(variousTypes, context.getSqlContext, outputSchema)
    outputModelDataframe
  }


    override def getName: String = "Store Various Types Transform"

  override def getTypeId: String = "storeVariousTypesTransform"

  override def getVersion: Version = new Version(0, 0, 1)

  override def getDescription: String = "Store Various Types Transform"

  override def getParameters: lang.Iterable[Parameter] = Lists.newArrayList[Parameter]()

}
