# koverse-spark-examples
Example code for using Spark with Koverse

## JoinTransform
This Transform shows how two input Collections can be joined together.
The field names to join on are Parameters so we again we avoid hardcoding.
Try running this on collections created from the `departments.csv` and `employees.csv` using the field name `departmentId` to join on.
See `src/main/scala/com/koverse/example/spark/JoinTransform.java`.

After running `mvn package`, this transform will be in the `target/koverse-spark-examples-0.1.0.jar` addon archive,
ready for upload to Koverse.

## Python Word Count Example
This is the Python take on the classic parallel computation of counting up words in a text corpus.

1. Create the `koverse_spark_examples` [conda](https://conda.io/docs/user-guide/install/download.html) environment from the `yml` file in the `python` directory of this repo: `conda env create --file python/koverse_spark_examples.yml --name koverse_spark_examples`
1. Activate the `koverse_spark_examples` conda environment (combination of commonly used Koverse and ML deps) - `conda activate koverse_spark_examples` - and while active:
    1. Install pyspark 1.6.3: `pip install git+https://github.com/jzerbe/spark.git@v1.6.3-pyspark#egg=pyspark`
1. Set the hadoop, spark, and the correct python executable paths (updating `~/.bashrc`):

        export HADOOP_HOME=/opt/hadoop-2.6.0
        export PYSPARK_PYTHON=/anaconda3/envs/koverse_spark_examples/bin/python
        export SPARK_HOME=/opt/spark-1.6.3-bin-hadoop2.6

1. Configure your IDE to use the newly created conda environment. If using IntelliJ, be sure to configure the default test runner to use Nosetests:

    ![](https://content.screencast.com/users/jason.zerbe/folders/Jing/media/66837538-1d93-4680-8dc5-32b529b59650/00000122.png)

1.  Now, you should be able to right click on a Python test and debug prior to deploying to a Koverse cluster.

As per [the documentation](https://koverse.readthedocs.io/en/2.8/dev/analytics/pyspark_transform.html),
the bare minimum of files you need in a `zip`
python addon archive are the `description.yaml` and `transform.py` files. The `transform.py` file
must contain a `PySparkTransform` class with an `__init__` and `execute` method.

This transform will be in `target/PySparkExampleAddon-bundle.zip` addon archive after running `mvn package`.

## WordCountTransform
Word Count, the "Hello World" of parallel computation, is shown being applied to the text in a specific field of Records.
The field name is a Parameter to the Transform so we can run this Tranform on Records in different Collections without
the field name being hardcoded. Try running this on a collection created from the `tweets.jsonstream`
example dataset using the field name `text`.
See `src/main/java/com/koverse/example/spark/JavaWordCountTransform.java`,
`src/main/scala/com/koverse/example/spark/WordCountTransform.scala`,
and `src/main/scala/com/koverse/example/spark/WordCountDataFrameTransform.scala`.

These transforms will be in the `target/koverse-spark-examples-0.1.0.jar` addon archive after running `mvn package`.

## JoinTransform
This Transform shows how two input Collections can be joined together. The field names to join on are Parameters so we again we avoid hardcoding. Try running this on collections created from the "departments.csv" and "employees.csv" using the field name "departmentId" to join on.

## NaiveBayesTrainedTransform

This transform uses Sparks' [NaiveBayesModel](https://spark.apache.org/docs/1.6.3/api/java/org/apache/spark/mllib/classification/NaiveBayesModel.html) from it's ML library to train a data set as seen [here](https://spark.apache.org/docs/1.6.3/mllib-naive-bayes.html) .


Weather Data Set (see [resource datasets](https://github.com/Koverse/koverse-spark-examples/blob/GS-569/src/main/resources/datasets/weather.csv) )

Weather, PlayTennis<br />
Overcast Cold Low Weak, 0<br />
Overcast Mild Low Strong, 0<br />
Sunny Mild Normal Weak, 0<br />
Rain Hot High Strong, 1<br />
Rain Mild Low Strong, 1<br />

Using the features of the dataset (i.e outlook, temperature, humidity, wind) predictions are made if you and a friend will play tennis.

The transform saves the model using `ObjectKoverseIO`'s objectToBytes (Java's [ByteArrayOutputStream](https://docs.oracle.com/javase/7/docs/api/java/io/ByteArrayOutputStream.html)/[ObjectOutputStream](https://docs.oracle.com/javase/7/docs/api/java/io/ObjectOutputStream.html)) function.<br />
This converts any Object to Byte Array so that it can be stored to Koverse's SimpleRecord.


## NaiveBayesPredictTransform

This transform reads in Sparks' Naive Bayes Model saved to a Koverse's SimpleRecord in `NaiveBayesTrainedTransform`.
The transform leverages `ObjectKoverseIO`'s objectFromBytes (Java's [ByteArrayInputStream](https://docs.oracle.com/javase/7/docs/api/java/io/ByteArrayInputStream.html)/[ObjectInputStream](https://docs.oracle.com/javase/7/docs/api/java/io/ObjectInputStream.html)) function.<br />
This converts a Byte Array to a type specified, this instance being a [NaiveBayesModel](https://spark.apache.org/docs/1.6.3/api/java/org/apache/spark/mllib/classification/NaiveBayesModel.html).
Once the NaiveBayesModel is successfully read then it can use the data saved for predictions to predict whether you and your friend will play tennis based on the weather.
The predictions are then stored to a SimpleRecord to Koverse.
