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

import com.koverse.sdk.Version;
import com.koverse.sdk.data.Parameter;
import com.koverse.sdk.data.SimpleRecord;
import com.koverse.sdk.transform.spark.JavaSparkTransform;
import com.koverse.sdk.transform.spark.JavaSparkTransformContext;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class JavaWordCountTransform extends JavaSparkTransform {

  private static final String TEXT_FIELD_NAME_PARAMETER = "textFieldName";
  
  /**
   * Koverse calls this method to execute your transform.
   *
   * @param context The context of this spark execution
   * @return The resulting RDD of this transform execution.
   *         It will be applied to the output collection.
   */
  @Override
  protected JavaRDD<SimpleRecord> execute(JavaSparkTransformContext context) {

    // This transform assumes there is a single input Data Collection
    String inputCollectionId = context.getInputCollectionIds().get(0);

    // Get the JavaRDD<SimpleRecord> that represents the input Data Collection
    JavaRDD<SimpleRecord> inputRecordsRdd = context.getInputCollectionRdds().get(inputCollectionId);

    // for each Record, tokenize the specified text field and count each occurrence
    final String fieldName = context.getParameters().get(TEXT_FIELD_NAME_PARAMETER);
    final JavaWordCounter wordCounter = new JavaWordCounter(fieldName, "['\".?!,:;\\s]+");
    
    return wordCounter.count(inputRecordsRdd);
  }

  /*
   * The following provide metadata about the Transform used for registration
   * and display in Koverse.
   */

  /**
   * Get the name of this transform. It must not be an empty string.
   *
   * @return The name of this transform.
   */
  @Override
  public String getName() {

    return "Java Word Count Example";
  }

  /**
   * Get the parameters of this transform.  The returned iterable can
   * be immutable, as it will not be altered.
   *
   * @return The parameters of this transform.
   */
  @Override
  public Iterable<Parameter> getParameters() {

    // This parameter will allow the user to input the field name of their Records which 
    // contains the strings that they want to tokenize and count the words from. By parameterizing
    // this field name, we can run this Transform on different Records in different Collections
    // without changing the code
    Parameter textParameter
            = new Parameter(TEXT_FIELD_NAME_PARAMETER, "Text Field Name", Parameter.TYPE_STRING);
    return Lists.newArrayList(textParameter);
  }

  /**
   * Get the programmatic identifier for this transform.  It must not
   * be an empty string and must contain only alpha numeric characters.
   *
   * @return The programmatic id of this transform.
   */
  @Override
  public String getTypeId() {

    return "javaWordCountExample";
  }

  /**
   * Get the version of this transform.
   *
   * @return The version of this transform.
   */
  @Override
  public Version getVersion() {

    return new Version(0, 0, 1);
  }

  /**
   * Get the description of this transform.
   *
   * @return The the description of this transform.
   */
  @Override
  public String getDescription() {
    return "This is the Java Word Count Example Transform";
  }
}
