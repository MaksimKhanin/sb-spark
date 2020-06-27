import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.types._

object test {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName(name = "Khanin.Lab07test").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val schema = StructType(
      List(
        StructField("uid", StringType , true),
        StructField("visits", ArrayType(
          StructType(
            List(
              StructField("url", StringType, true),
              StructField("timestamp", LongType, true)))))))

    val model = PipelineModel.load("lab07_model")

    val raw_kafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.0.1.13:6667")
      .option("subscribe", "maxim_khanin")
      .option("startingOffsets", """earliest""")
      .load

    val webLogs = raw_kafka.select(col("value").cast(StringType).as("json"))
      .select(from_json(col("json"), schema).as("data"))
      .select("data.*")
    val colUrl = (callUDF("parse_url", col("visits").getItem("url"), lit("HOST")).as("url"))
    val explodedWebLogs = webLogs.select(col("uid"), explode(col("visits")).as("visits"))
    val webVisits = explodedWebLogs.select(col("uid"), colUrl)
      .withColumn("domain", regexp_replace(col("url"), "www.", ""))
    val webVisitsList = webVisits.groupBy("uid").agg(collect_list("domain").as("domains"))

    val prediction = model.transform(webVisitsList)
      .select(col("uid"), col("category").as("gender_age"))
      .toJSON

    prediction
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.0.1.13:6667")
      .option("topic", "maxim_khanin_lab04b_out")
      .option("checkpointLocation", "lab07/checkpoint_out")
      .outputMode("update")
      .start()
      .awaitTermination()

    spark.stop()

  }
}
