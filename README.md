# koverse-spark-examples
Example code for using Spark with Koverse

## WordCountTransform
Word Count, the "Hello World" of parallel computation, is shown being applied to the text in a specific field of Records. The field name is a Parameter to the Transform so we can run this Tranform on Records in different Collections without the field name being hardcoded. Try running this on a collection created from the "tweets.jsonstream" example dataset using the field name "text".

## JoinTransform
This Transform shows how two input Collections can be joined together. The field names to join on are Parameters so we again we avoid hardcoding. Try running this on collections created from the "departments.csv" and "employees.csv" using the field name "departmentId" to join on.

## NaiveBayesTrainedTransform

This transform uses Sparks' Naive Bayes Model from it's ML library to train a data set as seen here https://spark.apache.org/docs/1.6.3/mllib-naive-bayes.html .

Weather Data Set (see resource datasets)

Weather,PlayTennis
Overcast Cold Low Weak,0
Overcast Mild Low Strong,0
Sunny Mild Normal Weak,0
Rain Hot High Strong,1
Rain Mild Low Strong,1

Using the features of the dataset (i.e outlook, temperature, humidity, wind) predictions are made if you and a friend will play tennis.

The transform saves the using ObjectKoverseIO's objectToBytes((Java's ByteArrayOutputStream/ObjectOutputStream)) function. This converts any Object to Byte Array so that it can be stored to Koverse's SimpleRecord.


## NaiveBayesPredictTransform

This transform reads in Sparks' Naive Bayes Model saved to a Koverse's SimpleRecord in NaiveBayesTrainedTransform.
The transform leverages ObjectKoverseIO's objectFromBytes (Java's ByteArrayInputStream/ObjectInputStream) function. This converts a Byte Array to a type specified, this instance being a NaiveBayesModel.
Once the NaiveBayesModel is successfully read then it can use the data saved for predictions to predict whether you and your friend will play tennis based on the weather.
The predictions are then stored to a SimpleRecord to Koverse.