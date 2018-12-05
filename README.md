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

Do note, that as per [the documentation](https://koverse.readthedocs.io/en/2.8/dev/analytics/pyspark_transform.html),
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

This transform uses Sparks' Naive Bayes Model from it's ML library to train a data set as seen [here](https://spark.apache.org/docs/1.6.3/mllib-naive-bayes.html) .


Weather Data Set (see [resource datasets](https://github.com/Koverse/koverse-spark-examples/blob/GS-569/src/main/resources/datasets/weather.csv) )

Weather,PlayTennis<br />
Overcast Cold Low Weak,0<br />
Overcast Mild Low Strong,0<br />
Sunny Mild Normal Weak,0<br />
Rain Hot High Strong,1<br />
Rain Mild Low Strong,1<br />

Using the features of the dataset (i.e outlook, temperature, humidity, wind) predictions are made if you and a friend will play tennis.

The transform saves the using `ObjectKoverseIO`'s objectToBytes((Java's `ByteArrayOutputStream`/`ObjectOutputStream`)) function. This converts any Object to Byte Array so that it can be stored to Koverse's SimpleRecord.


## NaiveBayesPredictTransform

This transform reads in Sparks' Naive Bayes Model saved to a Koverse's SimpleRecord in NaiveBayesTrainedTransform.
The transform leverages `ObjectKoverseIO`'s objectFromBytes (Java's `ByteArrayInputStream`/`ObjectInputStream`) function. This converts a Byte Array to a type specified, this instance being a NaiveBayesModel.
Once the NaiveBayesModel is successfully read then it can use the data saved for predictions to predict whether you and your friend will play tennis based on the weather.
The predictions are then stored to a SimpleRecord to Koverse.
