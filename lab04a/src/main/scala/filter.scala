import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object filter {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName(name = "Khanin.Lab04a").getOrCreate()

    val topic_name = spark.conf.get("spark.filter.topic_name")
    val offset = spark.conf.get("spark.filter.offset")
    val output_dir_prefix = spark.conf.get("spark.filter.output_dir_prefix")

    val schema = StructType(
      List(
        StructField("event_type", StringType, nullable = true),
        StructField("category", StringType,  nullable = true),
        StructField("item_id", StringType,  nullable = true),
        StructField("item_price", IntegerType,  nullable = true),
        StructField("uid", StringType,  nullable = true),
        StructField("timestamp", LongType,  nullable = true)
      )
    )

    val kafkaParams = Map(
      "kafka.bootstrap.servers" -> "10.0.1.13:6667",
      "subscribe" -> topic_name,
      "startingOffsets" -> offset,
      "maxOffsetsPerTrigger" -> "5"
    )

    val raw_kafka = spark.read.format("kafka").options(kafkaParams).load

    val parsedSdf = raw_kafka.select(col("value").cast(StringType).as("json"))
      .select(from_json(col("json"), schema).as("data"))
      .select("data.*")
      .withColumn("date", to_date((col("timestamp")/1000)
        .cast("timestamp"), "yyyyMMDD").cast("string"))
      .withColumn("date", regexp_replace(col("date"), lit("-"), lit("")))

    parsedSdf.write.partitionBy("event_type","date").parquet(output_dir_prefix)

    spark.stop()

  }
}
