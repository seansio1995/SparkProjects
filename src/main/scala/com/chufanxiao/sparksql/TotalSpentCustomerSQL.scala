package com.chufanxiao.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StringType, StructType}

object TotalSpentCustomerSQL {
  case class CustomerOrders(customer_id: Int, item_id: Int, amount_spent: Double)
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //create temperature schema
    val customerOrdersSchema = new StructType()
      .add("customer_id", IntegerType, nullable = true)
      .add("item_id", IntegerType, nullable = true)
      .add("amount_spent", DoubleType, nullable = true)


    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("WordCountSQL")
      .master("local[*]")
      .getOrCreate()

    // Infer the schema, and register the DataSet as a table.
    import spark.implicits._
    val lines = spark.read.schema(customerOrdersSchema).csv("./customer-orders.csv").as[CustomerOrders]

    val totalByCustomer = lines
      .groupBy("customer_id")
      .agg(round(sum("amount_spent"),2).alias("total_spent"))


    val totalByCustomerSort = totalByCustomer.sort(desc("total_spent"))

    totalByCustomerSort.show(totalByCustomerSort.count.toInt)

  }
}
