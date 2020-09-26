package com.chufanxiao.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FriendsbyAgeSQL {
  /** A function that splits a line of input into (age, numFriends) tuples. */
  def parseLine(line: String) = {
    // Split by commas
    val fields = line.split(",")
    // Extract the age and numFriends fields, and convert to integers
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    // Create a tuple that is our result.
    (age, numFriends)
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQLDemo")
      .master("local[*]")
      .getOrCreate()

    // Infer the schema, and register the DataSet as a table.
    import spark.implicits._
    val lines = spark.sparkContext.textFile("./fakefriends.csv")

    val people = lines.map(parseLine)

    val friendsByAge = people.toDS().withColumnRenamed("_1","age").withColumnRenamed("_2", "friends")

    friendsByAge.printSchema()

    friendsByAge.createOrReplaceTempView("friends")

    friendsByAge.show()

    //Raw
    friendsByAge.groupBy("age").avg("friends").show()

    //Round to 2 precision
    friendsByAge.groupBy("age").agg(round(avg("friends"),2)).sort("age").show()

    //With column name
    friendsByAge.groupBy("age").agg(round(avg("friends"),2).alias("friends_avg")).sort("age").show()

  }

}
