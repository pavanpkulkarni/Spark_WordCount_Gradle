package com.pavanpkulkarni.wordcount

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

object SparkWordCount {

  def main(args: Array[String]): Unit = {

    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
    val sc = new SparkContext(conf)
    

    //Read some example file to a test RDD
    val filename = args(0)
    val test = sc.textFile(filename)

    //test.flatMap { line => line.split(" ") }.map { word => (word, 1) }.reduceByKey(_ + _).saveAsTextFile("output.txt")

    val df = test.flatMap { line => line.split(" ") }.map { word => (word, 1) }.reduceByKey(_ + _)

    df.collect().take(20).foreach(println)

    //Stop the Spark context
    sc.stop

  }

}