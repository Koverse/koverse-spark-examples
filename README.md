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
