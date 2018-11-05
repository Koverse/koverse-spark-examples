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

import java._

import com.koverse.com.google.common.collect.Lists
import com.koverse.sdk.Version
import com.koverse.sdk.data.{Parameter, SimpleRecord}
import com.koverse.sdk.transform.spark.{JavaSparkTransform, JavaSparkTransformContext}
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.regression.LabeledPoint
import com.koverse.sdk.transform.spark.sql.{KoverseSparkSql}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.mllib.linalg.{SparseVector, Vectors}
import org.apache.spark.sql.DataFrame


class NaiveBayesTrainedTransform extends JavaSparkTransform {

  override def execute(context: JavaSparkTransformContext): JavaRDD[SimpleRecord] = {

    val inputCollectionId = context.getInputCollectionIds().get(0)
    val SQLContext = KoverseSparkSql.createSqlContext(context.getJavaSparkContext.sc)
    val inputDataFrame:DataFrame = KoverseSparkSql.createDataFrame(context.getInputCollectionRdds().get(inputCollectionId),
      SQLContext , context.getInputCollectionSchemas().get(inputCollectionId))

    val tokenizer:Tokenizer = new Tokenizer().setInputCol("Weather").setOutputCol("words")
    val wordsData:DataFrame = tokenizer.transform(inputDataFrame).drop("Weather")

    val hashTF:HashingTF = new HashingTF().setInputCol("words").setOutputCol("features").setNumFeatures(20)
    val featureData:DataFrame = hashTF.transform(wordsData)

    val dataDataFrame:DataFrame = featureData.select("PlayTennis","features")
    val dataRdd:JavaRDD[LabeledPoint] = dataDataFrame.map { f =>
        LabeledPoint(f.get(0).asInstanceOf[Double], Vectors.dense(f.get(1).asInstanceOf[SparseVector].toArray))
    }

    // Split data into training (60%) and test (40%).
    val splits: Array[JavaRDD[LabeledPoint]] = dataRdd.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training:JavaRDD[LabeledPoint]= splits(0)

    val model:NaiveBayesModel = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")
    //Writing the spark model to a byte array in order to store in Koverse
    val bytesModel = ObjectKoverseIO.objectToBytes(model)

    val simpleRecord:SimpleRecord = new SimpleRecord()
    simpleRecord.put("model", bytesModel)

   SQLContext.sparkContext.parallelize(Seq(simpleRecord)).toJavaRDD()
  }


  override def getName: String = "Naive Bayes Training Transform"

  override def getTypeId: String = "naiveBayesTrainedTransform"

  override def getVersion: Version = new Version(0, 0, 1)

  override def getDescription: String = "Naive Bayes Training Transform"

  override def getParameters: lang.Iterable[Parameter] = Lists.newArrayList[Parameter]()
}
