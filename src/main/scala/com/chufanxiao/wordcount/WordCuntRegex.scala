package com.chufanxiao.wordcount

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Count up how many of each word appears in a book as simply as possible. */
object WordCountRegex {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordCountRegex")

    // Read each line of my book into an RDD
    val input = sc.textFile("./book.txt")

    // Split into words separated by a space character, regex filter out punctuation
    val words = input.flatMap(x => x.split("\\W+"))

    // lower case words
    val lowerCaseWords = words.map(x => x.toLowerCase())

    // Count up the occurrences of each word
    val wordCounts = lowerCaseWords.map(x => (x,1)).reduceByKey((x,y) => x+y)

    // sort the count result by count desc order
    val sortedWordCounts = wordCounts.sortBy(_._2, ascending = false)

    // Print the results.
    //wordCounts.foreach(println)

    //Sort the word count result by Count value Desc order
    sortedWordCounts.collect().foreach(println)
  }

}


