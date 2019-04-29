package com.koverse.example.spark

import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row}

// Util class to convert the human readable input to/from
// double that Labeled point and vector can use
object NaiveBayesUtil {

  def getHumanReadableFeatures(features: Vector): String ={
    val doubleArray = features.toArray
    val outlook = doubleArray(0).toString match {case "1.0" => Weather.Sunny.toString case "2.0" => Weather.Overcast.toString case "3.0" => Weather.Rain.toString }
    val temperature = doubleArray(1).toString match {case "1.0" => Weather.Hot.toString case "2.0" => Weather.Mild.toString case "3.0" => Weather.Cold.toString }
    val humidity = doubleArray(2).toString match {case "1.0" => Weather.High.toString case "2.0" => Weather.Normal.toString case "3.0" => Weather.Low.toString }
    val wind = doubleArray(3).toString match {case "1.0" => Weather.Strong.toString case "2.0" => Weather.Weak.toString }
    outlook + ", " + temperature + ", " + humidity + ", " + wind
  }

  def getLabeledPoints(inputDataFrame: DataFrame): RDD[LabeledPoint] ={
    implicit val enc: Encoder[LabeledPoint] = Encoders.product[LabeledPoint]
    inputDataFrame.map{case(f:Row) =>
      val features = new Array[Double](4)
      features(0) = Weather.withNameOpt(f.getAs[String]("Outlook")) match { case Weather.Sunny => 1.0 case Weather.Overcast => 2.0 case Weather.Rain => 3.0} // Outlook
      features(1) = Weather.withNameOpt(f.getAs[String]("Temperature")) match {case Weather.Hot => 1.0 case Weather.Mild => 2.0 case Weather.Cold => 3.0} // Temperature
      features(2) = Weather.withNameOpt(f.getAs[String]("Humidity")) match { case Weather.High => 1.0 case Weather.Normal => 2.0 case Weather.Low => 3.0} // Humidity
      features(3) = Weather.withNameOpt(f.getAs[String]("Wind")) match {case Weather.Strong => 1.0 case Weather.Weak => 2.0} // Wind
    val classification = f.getAs[Double]("PlayTennis")
      val vector:Vector = new DenseVector(features)
      LabeledPoint(classification, vector)
    }.rdd
  }

}

object Weather extends Enumeration{
  type Weather = Value

  //  val Sunny, Overcast, Rainy, Hot, Mild, Cool, High, Low, Normal, Strong, Weak = Value

  //Outlook
  val Sunny = Value("Sunny")
  val Overcast = Value("Overcast")
  val Rain = Value("Rain")
  //temperature
  val Hot = Value("Hot")
  val Mild = Value("Mild")
  val Cold = Value("Cold")
  //humidity
  val High = Value("High")
  val Low = Value("Low")
  val Normal = Value("Normal")
  //wind
  val Strong = Value("Strong")
  val Weak = Value("Weak")
  val Unknown = Value("Unknown")

  def withNameOpt(s: String): Value = values.find(_.toString == s).getOrElse(Unknown)

}
