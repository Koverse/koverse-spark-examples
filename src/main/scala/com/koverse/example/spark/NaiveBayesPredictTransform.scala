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

import java.lang

import com.koverse.com.google.common.collect.Lists
import com.koverse.sdk.Version
import com.koverse.sdk.data.{Parameter, SimpleRecord}
import com.koverse.sdk.transform.spark.{JavaSparkTransform, JavaSparkTransformContext}
import com.koverse.sdk.transform.spark.sql.KoverseSparkSql
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

class NaiveBayesPredictTransform extends JavaSparkTransform {

  override def execute(context: JavaSparkTransformContext): JavaRDD[SimpleRecord] = {

    val SQLContext = KoverseSparkSql.createSqlContext(context.getJavaSparkContext.sc)
    val inputCollectionId = context.getInputCollectionIds().get(0)
    // Creating a data frame from input rdd because Spark's mllib Tokenizer class requires a data frame input
    val inputDataFrame:DataFrame = KoverseSparkSql.createDataFrame(context.getInputCollectionRdds().get(inputCollectionId),
      SQLContext , context.getInputCollectionSchemas().get(inputCollectionId))

    val modelId = context.getInputCollectionIds().get(1)
    val modelDataFrame:JavaRDD[SimpleRecord] = context.getInputCollectionRdds().get(modelId)

    // Test Data (40%)
    val testRDD: JavaRDD[LabeledPoint] = NaiveBayesHelper.generateLabeledPoints(inputDataFrame).randomSplit(Array(0.6, 0.4), seed = 11L)(1)

    //Reading model
    val byteModel:Array[Byte] = modelDataFrame.map{ case(modelRecord:SimpleRecord) =>
            modelRecord.get("model")(0)
    }.take(1)(0)
    //Converting the model in koverse record to a spark model
    val model:NaiveBayesModel = ObjectKoverseIO.objectFromBytes(byteModel, classOf[NaiveBayesModel])


    //Predictions on test data
      val predictionAndLabel:RDD[(Double, Double)] = testRDD.rdd.map{case (p) => (model.predict(p.features), p.label)}
      val accuracy:Double = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / testRDD.count()

      val predictions= predictionAndLabel.map{ case(x) =>
        val result = new SimpleRecord()
        result.put("prediction", x._1.toString)
        result.put("label", x._2.toString)
        result.put("accuracy", Double.box(accuracy))
        result
      }.collect().toSeq

     SQLContext.sparkContext.parallelize(predictions).toJavaRDD()
  }

  override def getName: String = "Naive Bayes Predict Transform"

  override def getTypeId: String = "naiveBayesPredictTransform"

  override def getVersion: Version = new Version(0, 0, 1)

  override def getDescription: String = "Naive Bayes Predict Transform"

  override def getParameters: lang.Iterable[Parameter] = Lists.newArrayList[Parameter]()
}
