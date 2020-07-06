import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.types._

object test_s {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName(name = "Khanin.Lab07Stest").getOrCreate()
    //spark.sparkContext.setLogLevel("ERROR")

    val schema = StructType(
      List(
        StructField("uid", StringType , true),
        StructField("visits", ArrayType(
          StructType(
            List(
              StructField("url", StringType, true),
              StructField("timestamp", LongType, true)))))))

    val pipe = PipelineModel.load("lab07S_model")

    val raw_kafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.0.1.13:6667")
      .option("subscribe", "maxim_khanin")
      .option("startingOffsets", """earliest""")
      .load

    val webLogs = raw_kafka.select(col("value").cast(StringType).as("json"))
      .select(from_json(col("json"), schema).as("data"))
      .select("data.*")

    val myParam = pipe.stages.last.getParam("model_path")
    val modelPath = pipe.stages.last.get(myParam).getOrElse(null).toString
    spark.sparkContext.addFile(modelPath, false)
    spark.sparkContext.addFile("transformer.py", false)
    val prediction = pipe.transform(webLogs).toJSON

    prediction
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.0.1.13:6667")
      .option("topic", "maxim_khanin_lab04b_out")
      .option("checkpointLocation", "lab07s/checkpoint_out")
      .outputMode("update")
      .start()
      .awaitTermination()

    spark.stop()

  }
}
