# koverse-spark-examples
Example code for using Spark with Koverse

## WordCountTransform
Word Count, the "Hello World" of parallel computation, is shown being applied to the text in a specific field of Records. The field name is a Parameter to the Transform so we can run this Tranform on Records in different Collections without the field name being hardcoded. Try running this on a collection created from the "tweets.jsonstream" example dataset using the field name "text".

## JoinTransform
This Transform shows how two input Collections can be joined together. The field names to join on are Parameters so we again we avoid hardcoding. Try running this on collections created from the "departments.csv" and "employees.csv" using the field name "departmentId" to join on.
