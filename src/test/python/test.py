import unittest

from koverse.transformTest import PySparkTransformTestRunner

from transform import PySparkTransform

text = ["There is a single instance of the word 'one'",
        "Unlike 'three', there are two instances of the word 'two'",
        "There are three instances of the word 'three'"]

class TestWordCountTransform(unittest.TestCase):

    def test_count_words(self):
        global text

        input_datasets = [[{'text': text}]]
        runner = PySparkTransformTestRunner({'textField': 'text'}, PySparkTransform)
        output_rdd = runner.testOnLocalData(inputDatasets)
        output = output_rdd.collect()

        self.assertTrue('word' in output[0])
        self.assertTrue('count' in output[0])

        

if __name__ == "__main__":
    unittest.main()

    def execute(self, context):
        inputDF = context.inputDataFrames.values()[0]

