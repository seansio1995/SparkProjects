package com.chufanxiao.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}

object MinTemp {
  case class Temperature(stationID: String, date: Int, measure_type: String, temperature: Float)
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //create temperature schema
    val tempSchema = new StructType()
      .add("stationId", StringType, nullable = true)
      .add("date", IntegerType, nullable = true)
      .add("measure_type", StringType, nullable = true)
      .add("temperature", FloatType, nullable = true)


    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("WordCountSQL")
      .master("local[*]")
      .getOrCreate()

    // Infer the schema, and register the DataSet as a table.
    import spark.implicits._
    val lines = spark.read.schema(tempSchema).csv("./1800.csv").as[Temperature]

    val minTemps = lines.filter($"measure_type" === "TMIN")

    val stationTemps = minTemps.select("stationId", "temperature")

    val minTempByStation = stationTemps.groupBy("stationId").min("temperature")

    val results = minTempByStation.collect()

    for(result <- results) {
      val station = result(0)
      val minTemp = result(1).asInstanceOf[Float] * 0.1f * 1.8f + 32.0f
      println(s"$station min temp: $minTemp")
    }
  }

}
