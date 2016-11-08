package com.koverse.example.spark;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.koverse.com.google.common.collect.ImmutableMap;
import com.koverse.com.google.common.collect.Lists;
import com.koverse.sdk.data.SimpleRecord;
import com.koverse.sdk.test.spark.SparkTransformTestRunner;

public class JavaWordCountTransformTest {

  @Test
  public void test() throws Throwable {
    
    SimpleRecord record0 = new SimpleRecord(
        ImmutableMap.<String, Object>of(
            "text", "these words are to be counted",
            "id", 0));
    SimpleRecord record1 = new SimpleRecord(
        ImmutableMap.<String, Object>of(
            "text", "more words that are worth counting",
            "id", 1));
    Map<String,String> parameterValues = ImmutableMap.<String,String>of("textFieldName", "text");
    
    List<SimpleRecord> inputRecords = Lists.newArrayList(record0, record1);
    
    
    List<SimpleRecord> outputRecords = 
        SparkTransformTestRunner.runTest(JavaWordCountTransform.class, 
            parameterValues, 
            ImmutableMap.<String,List<SimpleRecord>>of("testCollection", inputRecords), 
            "outputCollectionId");
    
    assertEquals(outputRecords.size(), 10L);
    
    
    
  }
}
