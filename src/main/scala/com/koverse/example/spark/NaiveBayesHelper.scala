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

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.mllib.linalg.{SparseVector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.DataFrame

object NaiveBayesHelper {

  def generateLabeledPoints(inputDataFrame: DataFrame): JavaRDD[LabeledPoint] ={
    val tokenizer:Tokenizer = new Tokenizer().setInputCol("Weather").setOutputCol("words")
    val wordsData:DataFrame = tokenizer.transform(inputDataFrame).drop("Weather")

    val hashTF:HashingTF = new HashingTF().setInputCol("words").setOutputCol("features").setNumFeatures(20)
    val featureData:DataFrame = hashTF.transform(wordsData)

    val dataDataFrame:DataFrame = featureData.select("PlayTennis","features")
    dataDataFrame.map { f =>
      LabeledPoint(f.get(0).asInstanceOf[Double], Vectors.dense(f.get(1).asInstanceOf[SparseVector].toArray))
    }
  }

}