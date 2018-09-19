#   Copyright 2018 Koverse, Inc.
#  
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#  
#     http://www.apache.org/licenses/LICENSE-2.0
#  
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

class PySparkTransform:

    def __init__(self, params):
        """params - a dict of data containing fields from the definitions in 'description.yaml' and populated from user specifications in Koverse"""
        self.text_field = params['text_field']

    def execute(self, context):
        pass

    def __count(self, rdd):
        """Perform a word count on the input rdd by tokenizing the text in the specified field.
        Return an rdd of (count,word) tuples
        """
        #Split the text in the records into lowercase words
        words = rdd.flatMap(lambda r: r[self.text_field].lower().split()) 

        #Generate pairs
        ones = words.keyBy(lambda _: 1)

        #Sum up the counts for each word
        wordCountRdd = ones.reduceByKey(lambda count, amount: count + amount)

        return wordCountRdd

        

