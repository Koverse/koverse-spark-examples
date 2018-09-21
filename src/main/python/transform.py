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
        input_rdd = list(context.inputRdds.values())[0]
        word_count_rdd = count(input_rdd, self.text_field)
        return word_count_rdd


def count(rdd, field_name):
    """Perform a word count on the specified field of the input rdd by tokenizing the text.
    Return an rdd of dicts with "word" and "count" fields.
    """
    #Split the text in the records into lowercase words
    words = rdd.flatMap(lambda r: r[field_name].lower().split()) 

    #Generate pairs
    ones = words.keyBy(lambda _: 1)

    #Sum up the counts for each word
    word_count_tuples = ones.reduceByKey(lambda count, amount: count + amount)

    #Convert to dicts
    word_count_dicts = word_count_tuples.map(lambda pair: {"word": pair[1], "count": pair[0]})

    return word_count_dicts
