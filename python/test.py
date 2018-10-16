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

import unittest

from koverse.transformTest import PySparkTransformTestRunner

from transform import PySparkTransform

text = ["There is a single instance of the word one",
        "Unlike three there are two instances of the word two",
        "There are three instances of the word three"]

class TestWordCountTransform(unittest.TestCase):

    def test_count_words(self):
        global text

        input_datasets = [[{'text': t} for t in text]]
        runner = PySparkTransformTestRunner({'text_field': 'text'}, PySparkTransform)
        output_rdd = runner.testOnLocalData(input_datasets)
        output = output_rdd.collect()

        self.assertTrue('word' in output[0])
        self.assertTrue('count' in output[0])

        ones = output_rdd.filter(lambda r: r['word'] == "one").collect()[0]
        twos = output_rdd.filter(lambda r: r['word'] == "two").collect()[0]
        threes = output_rdd.filter(lambda r: r['word'] == "three").collect()[0]
        
        self.assertEqual(ones['count'], 1)
        self.assertEqual(twos['count'], 2)
        self.assertEqual(threes['count'], 3)
        

if __name__ == "__main__":
    unittest.main()
