import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.PipelineModel

object dashboard {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName(name = "Khanin.Lab08").getOrCreate()

    val esLogin = spark.conf.get("spark.users_items.esLogin")
    val esPassword = spark.conf.get("spark.users_items.esPassword")
    val dataPath = spark.conf.get("spark.users_items.dataPath")
    val modelPath = spark.conf.get("spark.users_items.modelPath")
    val esOutputIndex = spark.conf.get("spark.users_items.esOutputIndex")

    val webLogs =  spark.read.json(dataPath)
    val model = PipelineModel.load(modelPath)

    val colUrl = (callUDF("parse_url", col("visits").getItem("url"), lit("HOST")).as("url"))
    val explodedWebLogs = webLogs.select(col("date"), col("uid"), explode(col("visits")).as("visits"))
    val webVisits = explodedWebLogs.select(col("date"), col("uid"), colUrl)
      .withColumn("domain", regexp_replace(col("url"), "www.", ""))
    val webVisitsList = webVisits.groupBy("date", "uid").agg(collect_list("domain").as("domains"))

    val prediction = model.transform(webVisitsList)
      .select(col("date"), col("uid"), col("category").as("gender_age"))

    prediction.write.format("es")
      .option("es.net.http.auth.user",esLogin)
      .option("es.net.http.auth.pass",esPassword)
      .option("es.nodes", "10.0.1.9")
      .option("es.port", "9200")
      .save(esOutputIndex+"/_doc")

  }

}
