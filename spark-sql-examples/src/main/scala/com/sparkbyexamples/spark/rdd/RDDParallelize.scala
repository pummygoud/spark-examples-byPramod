package com.sparkbyexamples.spark.rdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDDParallelize {

  def main(args: Array[String]): Unit = {


      val spark = SparkSession.builder()
        .master("local[1]")
        .appName("RDD Parallelize")
        .getOrCreate()


      val rdd: RDD[Int] = spark.sparkContext.parallelize(Array(1,2,3,4,5,6,7,8,9,10))
      val rddCollect: Array[Int] = rdd.collect()

      println("Number of Partitions: "+ rdd.getNumPartitions)
      println("Action: First Element: " + rdd.first())
      println("Action: RDD Converted to Array[Int]:")

      rddCollect.foreach(println)

      val emptyRDD = spark.sparkContext.parallelize(Seq.empty[String])
    println("Empty RDD: ")
    emptyRDD.foreach(println)

  }
}
