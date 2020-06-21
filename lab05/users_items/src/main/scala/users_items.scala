import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import org.apache.spark.sql.functions._

object users_items {
  def main(args: Array[String]): Unit = {

    val normString = (colVal: Column) => {
      lower(regexp_replace(trim(colVal), "[^a-zA-Z0-9\\s]", "_"))
    }

    val dfToMatrix = (df: DataFrame, groupColNm: String, pivotColNm: String,  Prefix: String) => {
      df.withColumn(pivotColNm, normString(col(pivotColNm)))
        .withColumn(pivotColNm, concat(lit(Prefix), col(pivotColNm)))
        .groupBy(groupColNm).pivot(pivotColNm).count()
    }

    val spark = SparkSession.builder().appName(name = "Khanin.Lab05").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val confOutputDir = spark.conf.get("spark.users_items.output_dir")
    val confInputDir = spark.conf.get("spark.users_items.input_dir")
    val confUpdate = if (spark.conf.get("spark.users_items.update") == "1") 1 else 0

    println(s"Update param is $confUpdate")
    println(s"confInputDir param is $confInputDir")
    println(s"confOutputDir param is $confOutputDir")

    val views = spark.read.json(confInputDir + "/view/")
    val buys = spark.read.json(confInputDir + "/buy/")
    val uidDates = views.select("uid", "date").union(buys.select("uid", "date"))
    val maxInputDate= uidDates.agg(max("date")).collect()(0)(0).toString
    val users = uidDates.select("uid").distinct
    val buyMatrix = dfToMatrix(buys.withColumn("item_id", normString(col("item_id"))), "uid", "item_id", "buy_")
    val viewsMatrix = dfToMatrix(views.withColumn("item_id", normString(col("item_id"))), "uid", "item_id", "view_")
    val input_matrix = users.join(buyMatrix, Seq("uid"), "left").join(viewsMatrix, Seq("uid"), "left").na.fill(0)


    println(s"maxInputDate param is $maxInputDate")

    if (confUpdate != 1)
    {
      println(s"confUpdate != 0 param is used")
      input_matrix.write.parquet(confOutputDir+"/"+maxInputDate)
      println(s"Matrix has been written")
    }
    else
    {
      println(s"confUpdate == 1 param is used")
      val previousMatrix = spark.read.parquet(confOutputDir+"/"+maxInputDate)

      val newMatrix = previousMatrix.union(input_matrix).na.fill(0)
      val newOutputDir = confOutputDir+"/"+maxInputDate
      println(s"MaxOutData param is $newOutputDir")

      newMatrix.write.parquet(newOutputDir)
      println(s"Matrix has been written")
    }
    spark.stop()


  }

}
