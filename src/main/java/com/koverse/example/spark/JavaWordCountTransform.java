package com.koverse.example.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.koverse.com.google.common.collect.Lists;
import com.koverse.sdk.Version;
import com.koverse.sdk.data.Parameter;
import com.koverse.sdk.data.SimpleRecord;
import com.koverse.sdk.transform.spark.JavaSparkTransform;
import com.koverse.sdk.transform.spark.JavaSparkTransformContext;

public class JavaWordCountTransform extends JavaSparkTransform {

    private final String TEXT_FIELD_NAME_PARAMETER = "textFieldName";

    @Override
    protected JavaRDD<SimpleRecord> execute(JavaSparkTransformContext context) {

        // This transform assumes there is a single input Data Collection
        String inputCollectionId = context.getInputCollectionIds().get(0);

        // Get the JavaRDD<SimpleRecord> that represents the input Data Collection
        JavaRDD<SimpleRecord> inputRecordsRdd = context.getInputCollectionRdds().get(inputCollectionId);

        // for each Record, tokenize the specified text field and count each occurence
        final String fieldName = context.getParameters().get(TEXT_FIELD_NAME_PARAMETER);
        JavaRDD<String> words = inputRecordsRdd.flatMap(new FlatMapFunction<SimpleRecord, String>() {
            @Override
            public Iterable<String> call(SimpleRecord record) {
                return Lists.newArrayList(record.get(fieldName).toString().split("['\".?!,:;\\s]"));
            }
        });

        // combine the lower casing of the string with generating the pairs.
        JavaPairRDD<String, Integer> ones
                = words.mapToPair(new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<String, Integer>(s.toLowerCase().trim(), 1);
                    }
                });

        JavaPairRDD<String, Integer> wordCountRdd
                = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });

        // turn each tuple into an output Record with a "word" and "count" fields
        JavaRDD<SimpleRecord> outputRdd
                = wordCountRdd.map(new Function<Tuple2<String, Integer>, SimpleRecord>() {

                    @Override
                    public SimpleRecord call(Tuple2<String, Integer> wordCount) {
                        SimpleRecord record = new SimpleRecord();
                        record.put("word", wordCount._1);
                        record.put("count", wordCount._2);
                        return record;
                    }
                });

        return outputRdd;
    }

    /**
     * The following provide metadata about the Transform used for registration
     * and display in Koverse
     */
    public String getName() {

        return "Java Word Count Example";
    }

    public Iterable<Parameter> getParameters() {

        // This parameter will allow the user to input the field name of their Records which 
        // contains the strings that they want to tokenize and count the words from. By parameterizing
        // this field name, we can run this Transform on different Records in different Collections
        // without changing the code
        Parameter textParameter
                = new Parameter(TEXT_FIELD_NAME_PARAMETER, "Text Field Name", Parameter.TYPE_STRING);
        return Lists.newArrayList(textParameter);
    }

    public String getTypeId() {

        return "javaWordCountExample";
    }

    public Version getVersion() {

        return new Version(0, 0, 1);
    }

    public String getDescription() {
        return "This is the Java Word Count Example Transform";
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
