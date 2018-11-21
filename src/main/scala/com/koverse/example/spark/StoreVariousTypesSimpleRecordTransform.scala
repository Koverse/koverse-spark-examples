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
import com.koverse.sdk.data.{Parameter, SimpleRecord}
import com.koverse.sdk.transform.spark.{JavaSparkTransform, JavaSparkTransformContext}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.util.Random

class StoreVariousTypesSimpleRecordTransform extends JavaSparkTransform{

  override def execute(context: JavaSparkTransformContext): JavaRDD[SimpleRecord] = {

    context.getJavaSparkContext.setLogLevel("INFO")


    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val date = formatter.parse("2018-11-07")
    val sqlDate = new sql.Date(date.getTime)
    val timestamp = new Timestamp(System.currentTimeMillis())
    //      val exampleShort:Short = 1
    val binaryType = Array(45, 21, 4, 5, 6).map(_.toByte)
    //      val calendarInterval: CalendarInterval = new CalendarInterval(1, 123000L)
    val mapType: java.util.Map[Double, String] = new java.util.HashMap[Double, String]
    mapType.put(2.8, "Koverse")
    val intArrayType = Array(Int.box(43))
    val nestedArray: Array[Array[Double]] = Array(Array(1.0))

    val simpleRecord = new SimpleRecord()
      simpleRecord.put("String","Koverse") //String
      simpleRecord.put("Binary", binaryType) //Binary
      simpleRecord.put("Boolean",true) //Boolean
      //        calendarInterval,//CalendarIntervalType
      simpleRecord.put("SQLDateType",sqlDate) //DateType
      //        null, //NullType
      simpleRecord.put("TimestampType", timestamp) //TimestampType
//      simpleRecord.put("Array",intArrayType) //ArrayType
      //        mapType,//MapType
//      simpleRecord.put("NestedArray", nestedArray) //Nested ArrayType
      //        data.get(2).asInstanceOf[String].getBytes(),//ByteType
      simpleRecord.put("Double", Random.nextDouble()) //DoubleType
      //        data.get(2).asInstanceOf[String].getBytes()(0),
      simpleRecord.put("Integer", Random.nextInt()) //IntegerType
      //        Random.nextFloat(),//FloatType
      simpleRecord.put("Long", Random.nextLong()) //LongType
      //        exampleShort,//ShortType
//      simpleRecord.put("DecimalType",BigDecimal.double2bigDecimal(Random.nextDouble())) //DecimalType

    val variousTypes:JavaRDD[SimpleRecord] = context.getJavaSparkContext.sc.parallelize(Seq(simpleRecord)).toJavaRDD()
//
//    val outputSchema: StructType = StructType(Seq(
//      //DataTypes
//      StructField("StringType", StringType, true),
//      StructField("BinaryType", BinaryType, true),
//      StructField("BooleanType", BooleanType, true),
//      //      StructField("CalendarIntervalType", CalendarIntervalType, true),
//      StructField("DateType", DateType, true),
//      //      StructField("NullType", NullType, true),
//      //      StructField("StructType",  StructType, true),
//      StructField("TimestampType", TimestampType, true),
//      //      StructField("UserDefinedType", UserDefinedType, true), VectorUDT
//      StructField("ArrayType", ArrayType(IntegerType, true), true),
//      //      StructField("MapType", DataTypes.createMapType(DoubleType, StringType), true),
//      StructField("NestedArrayType", ArrayType(ArrayType(DoubleType, true)), true),
//      //Numeric Types
//      StructField("DoubleType", DoubleType, true),
//      //      StructField("ByteType", ByteType, true),
//      StructField("IntegerType", IntegerType, true),
//      //      StructField("FloatType", FloatType, true),
//      StructField("LongType", LongType, true),
//      //      StructField("ShortType", ShortType, true),
//      //Decimal Type
//      StructField("DecimalType", DecimalType(32, 16), true)
//      //Object Types
//      //      StructField("ObjectType", ObjectType, true),
//      //      StructField("Metadata", Metadata, true),
//      //      StructField("MetadataBuilder", MetadataBuilder, true),
//      //      StructField("PrecisionInfo", PrecisionInfo, true),
//      //      StructField("StructField", StructField, true),
//      //      StructField("AnyDataType", AnyDataType, true)
//
//    ))
    variousTypes
  }


  override def getName: String = "Store Various Types Koverse Record Transform"

  override def getTypeId: String = "storeVariousTypesKoverseRecordTransform"

  override def getVersion: Version = new Version(0, 0, 1)

  override def getDescription: String = "Store Various Types Koverse Record Transform"

  override def getParameters: lang.Iterable[Parameter] = Lists.newArrayList[Parameter]()
}