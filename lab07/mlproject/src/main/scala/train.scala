import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{CountVectorizer, StringIndexer, IndexToString}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.Pipeline

object train {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName(name = "Khanin.Lab07train").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val webLogs = spark.read.json("/labs/laba07")

    val colUrl = (callUDF("parse_url", col("visits").getItem("url"), lit("HOST")).as("url"))
    val explodedWebLogs = webLogs.select(col("uid"), col("gender_age"), explode(col("visits")).as("visits"))
    val webVisits = explodedWebLogs.select(col("uid"), colUrl, col("gender_age"))
      .withColumn("domain", regexp_replace(col("url"), "www.", ""))
    val webVisitsList = webVisits.groupBy("uid", "gender_age").agg(collect_list("domain").as("domains"))

    val cv = new CountVectorizer()
      .setInputCol("domains")
      .setOutputCol("features")

    val indexer = new StringIndexer()
      .setInputCol("gender_age")
      .setOutputCol("label")
    val labels = indexer.fit(webVisitsList).labels

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)

    val indexToStringEstimator = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("category")
      .setLabels(labels)

    val pipeline = new Pipeline()
      .setStages(Array(cv, indexer, lr, indexToStringEstimator))

    val model = pipeline.fit(webVisitsList)

    model.write.overwrite().save("lab07_model")

    spark.stop()

  }
}
