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

package com.koverse.example.spark;

import com.koverse.com.google.common.collect.Lists;

import com.koverse.sdk.data.SimpleRecord;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class JavaWordCounter implements java.io.Serializable {

  private static final long serialVersionUID = 8741666028339586272L;
  private final String textFieldName;
  private final String tokenizationString;

  public JavaWordCounter(String textFieldName, String tokenizationString) {
    this.textFieldName = textFieldName;
    this.tokenizationString = tokenizationString;
  }

  /**
   * Performs a word count on the inputRecordsRdd by tokenizing.
   * the text in the textFieldName field
   * @param inputRecordsRdd input RDD of SimpleRecords
   * @return a JavaRDD of SimpleRecords that have "word" and "count"
   *     fields in each record
   */
  public JavaRDD<SimpleRecord> count(JavaRDD<SimpleRecord> inputRecordsRdd) {

    // split the text in the records into words
    JavaRDD<String> words = inputRecordsRdd.flatMap(record -> {
      return Lists.newArrayList(record.get(textFieldName).toString()
          .split(tokenizationString));
    });

    // combine the lower casing of the string with generating the pairs.
    JavaPairRDD<String, Integer> ones = words.mapToPair(word -> {
      return new Tuple2<String, Integer>(word.toLowerCase().trim(), 1);
    });

    // sum up the counts for each word
    JavaPairRDD<String, Integer> wordCountRdd = ones
        .reduceByKey((count, amount) -> count + amount);

    // turn each tuple into an output Record with a "word" and "count" field
    JavaRDD<SimpleRecord> outputRdd = wordCountRdd.map(wordCountTuple -> {
      SimpleRecord record = new SimpleRecord();
      record.put("word", wordCountTuple._1);
      record.put("count", wordCountTuple._2);
      return record;
    });

    return outputRdd;

  }
}
