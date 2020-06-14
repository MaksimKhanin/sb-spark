package com.sberbankde

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object agg {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName(name = "Khanin.Lab04b").getOrCreate()

    val schema = StructType(
      List(
        StructField("event_type", StringType, nullable = true),
        StructField("category", StringType, nullable = true),
        StructField("item_id", StringType, nullable = true),
        StructField("item_price", IntegerType, nullable = true),
        StructField("uid", StringType, nullable = true),
        StructField("timestamp", LongType, nullable = true)
      )
    )

    val raw_kafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.0.1.13:6667")
      .option("subscribe", "maxim_khanin")
      .option("startingOffsets", """earliest""")
      //.option("maxOffsetsPerTrigger", "5")
      .option("checkpointLocation", "lab4b/checkpoint_in")
      .load

    val parsedSdf = raw_kafka.select(col("value").cast(StringType).as("json"))
      .select(from_json(col("json"), schema).as("data"))
      .select("data.*")
      .withColumn("timestamp", (col("timestamp") / 1000).cast("timestamp"))


    val stream_data = parsedSdf
      .withWatermark("timestamp", "2 hour")
      .groupBy(window(col("timestamp"), "1 hour", "1 hour").as("window_tf"))
      .agg(
        sum(when(col("event_type") === lit("buy"), col("item_price"))).as("revenue"),
        count(when(col("uid").isNotNull, col("uid"))).as("visitors"),
        count(when(col("event_type") === lit("buy"), col("event_type"))).as("purchases"),
        avg(when(col("event_type") === lit("buy"), col("item_price"))).as("aov")
      )
      .select(col("window_tf").getField("start").cast("long").as("start_ts"),
        col("window_tf").getField("end").cast("long").as("end_ts"),
        col("revenue"), col("visitors"),
        col("purchases"), col("aov"))
      .toJSON

    val stream_kafka = stream_data
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.0.1.13:6667")
      .option("topic", "maxim_khanin_lab04b_out")
      .option("checkpointLocation", "lab4b/checkpoint_out")
      .outputMode("update")
      .start()
      .awaitTermination()
//
//
//    val stream_console = stream_data
//      .writeStream
//      .format("console")
//      .option("truncate", "false")
//      .outputMode("update")
//      .start()
//      .awaitTermination()

    spark.stop()
  }
}
