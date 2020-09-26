package com.chufanxiao.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WordCountSQL {
  /** A . */
  case class Book(value : String)

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("WordCountSQL")
      .master("local[*]")
      .getOrCreate()

    // Infer the schema, and register the DataSet as a table.
    import spark.implicits._
    val lines = spark.read.text("./book.txt").as[Book]

    //Split into words
    val words = lines.select(explode(split($"value", "\\W+")).alias("word"))
      .filter($"word" =!= "")

    //Normalize to lowercase
    val lowercaseWords = words.select(lower($"word").alias("word"))

    //Count
    val wordCounts = lowercaseWords.groupBy("word").count()

    //Sort the count
    val wordCountsSorted = wordCounts.sort(desc("count"))

    //Show result
    wordCountsSorted.show(wordCountsSorted.count.toInt)

    //Another way TO DO using RDD and Dataset
    val bookRDD = spark.sparkContext.textFile("./book.txt")
    val wordsRDD = bookRDD.flatMap(x => x.split("\\W+"))
    val wordsDS = wordsRDD.toDS()

    val lowercaseWordsDS = wordsDS.select(lower($"value").alias("word"))
    val wordCountDS = lowercaseWordsDS.groupBy("word").count()
    val wordCountDSSorted = wordCountDS.sort("count")
    wordCountDSSorted.show(wordCountDSSorted.count.toInt)


  }
}
