

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import sys.process._
import org.apache.log4j._
//import org.elasticsearch.spark.sql
//import org.postgresql._
//import com.datastax.spark.connector._
//import org.apache.spark.sql.cassandra._
//import com.datastax.spark.connector.cql.CassandraConnectorConf
//import com.datastax.spark.connector.rdd.ReadConf



object data_mart {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName(name = "Khanin.Lab03").getOrCreate()


    Logger.getLogger("org").setLevel(Level.ERROR)


    val normString = udf((colVal: String) => {
      colVal.replaceAll("[.-]", "_").toLowerCase()
    })
    val AgeBins = udf((age: Integer) => {
      age match {
        case age if 18 to 24 contains age => "18-24"
        case age if 25 to 34 contains age => "25-34"
        case age if 35 to 44 contains age => "35-44"
        case age if 45 to 54 contains age => "45-54"
        case age if age >= 55 => ">=55"
        case _ => "other"
      }
    })

    val webLogs = spark.read.json("/labs/laba03/weblogs.json")

    //    val esOptions =
    //      Map(
    //        "es.nodes" -> "10.0.1.9:9200",
    //        "es.batch.write.refresh" -> "false",
    //        "es.nodes.wan.only" -> "true"
    //      )
    val esLogs = spark.read.format("es")
      .option("es.nodes", "10.0.1.9")
      .option("es.port", "9200")
      .load("visits*")
    println("elasticdone")
    spark.conf.set("spark.cassandra.connection.host", "10.0.1.9")
    spark.conf.set("spark.cassandra.connection.port", "9042")
    spark.conf.set("spark.cassandra.output.consistency.level", "ANY")
    spark.conf.set("spark.cassandra.input.consistency.level", "ONE")
    val tableOpts = Map("table" -> "clients","keyspace" -> "labdata")

    println("cassandrastart")
    val clientInfo = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(tableOpts)
      .load()
    println("cassandraloaded")
    val clientInfoBinned = clientInfo.withColumn("age_cat", AgeBins(col("age")))
      .select(col("uid"), col("age_cat"), col("gender"))
    println("postgreStart")
    val pwd = "0DPAFTVJ"
    val domainCats = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:postgresql://10.0.1.9:5432/labdata")
      .option("dbtable", "domain_cats")
      .option("user", "maxim_khanin")
      .option("password", pwd)
      .option("driver", "org.postgresql.Driver")
      .load()

    val normDomainCats = broadcast(domainCats.withColumn("category", concat(lit("web_"), normString(col("category")))))

    val explodedWebLogs = webLogs.select(col("uid"), explode(col("visits")).as("visits"))

    val urlExtracted = explodedWebLogs.select(col("uid"), col("visits").getItem("url").as("url"))
    val webVisits = urlExtracted.withColumn("url", regexp_replace(col("url"), "http://http://", "http://"))
      .withColumn("url", regexp_replace(col("url"), "www.", ""))
      .withColumn("url", lower(trim(regexp_replace(col("url"), "\\s+", " "))))
      .withColumn("domain", callUDF("parse_url", col("url"), lit("HOST")))
      .select("uid", "domain")
    val webCatVisits = webVisits.join(normDomainCats, Seq("domain"), "inner").select("uid", "category")
    val pivotWebVistis = webCatVisits.groupBy("uid").pivot("category").count()

    val shopCatVisits = esLogs.filter(col("uid").isNotNull)
      .withColumn("category", concat(lit("shop_"), normString(col("category"))))
      .select("uid", "category")

    val pivotShopVistis = shopCatVisits.groupBy("uid").pivot("category").count()

    val fin = clientInfoBinned.join(pivotWebVistis, Seq("uid"), "left")
      .join(pivotShopVistis, Seq("uid"), "left").na.fill(0)
    println("postgreWrite")
    fin.write
      .format("jdbc")
      .option("url", "jdbc:postgresql://10.0.1.9:5432/maxim_khanin")
      .option("dbtable", "clients")
      .option("user", "maxim_khanin")
      .option("password", pwd)
      .option("driver", "org.postgresql.Driver")
      .save()
  }
}
