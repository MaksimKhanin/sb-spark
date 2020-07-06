package org.sberbank.lab07s

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.ml.Pipeline

object train {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName(name = "Khanin.Lab07Strain").getOrCreate()
    //spark.sparkContext.setLogLevel("ERROR")
    val vocabSize = 1000
    val webLogs = spark.read.json("/labs/laba07")
    spark.sparkContext.addFile("fit.py", false)
    val urlExtractor = new(Url2DomainTransformer)
      .setInputCol("visits")
      .setOutputCol("domain")

    val cv = new CountVectorizer()
      .setInputCol("domain")
      .setOutputCol("features")
      .setVocabSize(vocabSize)

    val lr = new SklearnEstimator()
      .setInputCol("features")
      .setOutputCol("gender_age")
      .setVocabSize(vocabSize)
      .setModelPath("lab07.model")

    val pipeline = new Pipeline()
      .setStages(Array(urlExtractor, cv, lr))

    pipeline.fit(webLogs)

    spark.stop()

  }
}