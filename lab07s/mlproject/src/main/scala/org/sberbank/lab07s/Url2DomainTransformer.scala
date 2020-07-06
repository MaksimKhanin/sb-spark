package org.sberbank.lab07s

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class Url2DomainTransformer(override val uid: String) extends Transformer
  with HasInputCol
  with HasOutputCol
  with DefaultParamsReadable[this.type]
  with DefaultParamsWritable{

  def setInputCol(value: String): this.type = set(inputCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)
  def this() = this(Identifiable.randomUID("url2domaintransformer"))

  def copy(extra: ParamMap): Url2DomainTransformer = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {

    val requiredSchema =
      StructField($(inputCol), ArrayType(
        StructType(
          List(
            StructField("timestamp", LongType, true),
            StructField("url", StringType, true)))))

    val actualDataType = schema($(inputCol))
    require(actualDataType.equals(requiredSchema),
      s"Schema ${$(inputCol)} must be $requiredSchema but provided $actualDataType")
    schema.add(StructField($(outputCol), ArrayType(DataTypes.StringType)))

  }

  def transform(df: Dataset[_]): DataFrame = {

    val colUrl = (callUDF("parse_url", col($(inputCol)).getItem("url"), lit("HOST")).as("url"))
    val explodedWebLogs = df.select(col("uid"), col("gender_age"), explode(col("visits")).as("visits"))
    val webVisits = explodedWebLogs.select(col("uid"), colUrl, col("gender_age"))
      .withColumn("domain", regexp_replace(col("url"), "www.", ""))
    webVisits.groupBy("uid", "gender_age").agg(collect_list("domain").as($(outputCol)))
  }
}