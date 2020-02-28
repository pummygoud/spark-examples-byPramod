package com.sparkbyexamples.spark.rdd

import org.apache.spark.sql.SparkSession

object CreateEmptyRDD {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .appName("Creating Simple RDDs")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val rdd = spark.sparkContext.emptyRDD
    val rddString = spark.sparkContext.emptyRDD[String]

    println(rdd)
    println(rddString)
    println("Num of Partitions: "+rdd.getNumPartitions)

    //rddString.saveAsTextFile("test.txt") // returns error

    val rdd2 = spark.sparkContext.parallelize(Seq.empty[String])
    println(rdd2)
    println("Num of Partitions: "+rdd2.getNumPartitions)

    //rdd2.saveAsTextFile("test3.txt")

    // Pair RDD

    type dataType = (String,Int)
    var pairRDD = spark.sparkContext.emptyRDD[dataType]
    println(pairRDD)


    val rdd5 = spark.sparkContext.parallelize(Seq((1, "Apple"),(2, "Amazon"),(3, "Google"),(4, "Microsoft")))

    rdd5.foreach(println)
  }
}
