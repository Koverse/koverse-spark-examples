from __future__ import print_function

import os
import sys

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import split
from pyspark.sql.functions import explode
from pyspark.sql.functions import lower

# spark-submit --master yarn --repositories http://nexus.koverse.com/nexus/content/groups/public/ --packages com.koverse:koverse-spark-datasource:2.3.0 word_count.py
# OR
# spark-submit --master yarn --jars /path/to/koverse-spark-datasource-2.3.0.jar word_count.py

if __name__ == "__main__":

    if len(sys.argv) != 5:
        print("Usage: word_count <koverse_host> <api_token> <data set name> <text column>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="Koverse PySpark Word Count Example")
    sqlContext = SQLContext(sc)

    # Load the Koverse Data Set into a Spark DataFrame
    # This requires the Koverse Spark Data Source be available by either
    # referencing the JAR locally with --jars or using the --repositories and
    # --packages arguments of spark-submit
    dataFrame = sqlContext.read.format('com.koverse.spark').options(hostname=sys.argv[1], apiToken=sys.argv[2]).load(sys.argv[3])
    splitDataFrame = dataFrame.select(split(sys.argv[4], '''['".?!,:;\s]''').alias('s'))
    wordDataFrame = splitDataFrame.select(explode('s').alias('word'))
    countDataFrame = wordDataFrame.select(lower(wordDataFrame.word).alias('lowerWord')).groupBy('lowerWord').count()

    countDataFrame.sort('count', ascending=False).show()

    # Write support pull request needs to be merged, then the below will work
    # countDataFrame.write.format('com.koverse.spark').options(hostname=sys.argv[1], apiToken=sys.argv[2]).save('word count')

    sc.stop()
