package com.sparkbyexamples.spark.practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OrdersByDateAndStatus {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .appName("Orders by OrderDate and Status")
      .getOrCreate()

    spark.sqlContext.setConf("hive.metastore.uris","thrift://localhost:9083")
    spark.sqlContext.setConf("spark.hadoop.fs.defaultFS","hdfs://quickstart.cloudera:8020")

    Logger.getLogger("org").setLevel(Level.OFF)

    val ordersDF = spark.read.format("jdbc")
      .option("url","jdbc:mysql://localhost/retail_db")
      .option("user", "root")
      .option("password", "cloudera")
      .option("dbtable", "orders")
      .option("driver","com.mysql.jdbc.Driver")
      .load()

    val orderItemsDF = spark.read.format("jdbc")
      .option("url","jdbc:mysql://localhost/retail_db")
      .option("user", "root")
      .option("password", "cloudera")
      .option("dbtable", "order_items")
      .option("driver","com.mysql.jdbc.Driver")
      .load()

    //ordersDF.show()

    //orderItemsDF.show()
    import spark.implicits._

    val joinedOrders = ordersDF.join(orderItemsDF, ordersDF("order_id") === orderItemsDF("order_item_order_id"))
      .select(col("order_id"), col("order_date"),col("order_status"),col("order_item_subtotal"))

    //joinedOrders.show()

    val groupedOrders = joinedOrders.groupBy(col("order_date"),col("order_status"))
      .agg(round(sum(col("order_item_subtotal")),2).as("total_amount"),
        countDistinct(col("order_id")).as("total_orders"))
      .orderBy(col("order_date").desc, col("total_orders").desc)

    //groupedOrders.show()
    val finalOutput = groupedOrders.coalesce(4).cache()

    // Save as file with parquet
    // compression codec gzip
    spark.sqlContext.setConf("spark.sql.parquet.compression.codec","gzip")
    finalOutput.write.parquet("hdfs://quickstart.cloudera:8020/user/cloudera/data/solutions/Intellij/OrdersByDateAndStatus/parquet-gzip")

    // Save as file with parquet
    // compression codec gzip
    spark.sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
    finalOutput.write.parquet("hdfs://quickstart.cloudera:8020/user/cloudera/data/solutions/Intellij/OrdersByDateAndStatus/parquet-snappy")

    spark.sqlContext.setConf("spark.sql.parquet.compression.codec","uncompressed")
    finalOutput.write.parquet("hdfs://quickstart.cloudera:8020/user/cloudera/data/solutions/Intellij/OrdersByDateAndStatus/parquet-uncompressed")
  }
}
