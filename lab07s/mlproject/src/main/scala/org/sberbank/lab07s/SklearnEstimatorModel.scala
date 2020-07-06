package org.sberbank.lab07s

import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.ml.Model
import org.apache.spark.ml.linalg.SQLDataTypes
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.DefaultParamsReadable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, regexp_replace, split, udf}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class SklearnEstimatorModel(override val uid: String) extends Model[SklearnEstimatorModel]
  with SklearnEstimatorParams
  with HasSklearnModel
  with DefaultParamsReadable[this.type]{

  override def copy(extra: ParamMap): SklearnEstimatorModel = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {

    val requiredSchema = StructField($(inputCol), SQLDataTypes.VectorType, false)
    val actualDataType = schema($(inputCol))
    require(actualDataType.equals(requiredSchema),
      s"Schema ${$(inputCol)} must be $requiredSchema but provided $actualDataType")
    schema.add(StructField($(outputCol), StringType, false))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    import dataset.sparkSession.implicits._
    val toDense = udf((v:MLVector) => v.toDense.toArray)
    val df = dataset.withColumn( $(inputCol), toDense(col( $(inputCol))))
      .select("uid",  $(inputCol))

    val arrayDFColumn = df.select(
      df("uid") +: (0 until $(vocabSize)).map(i => df($(inputCol))(i).alias(s"feature_$i")): _*
    )
    //spark.sparkContext.addFile("lab07.model", true)
    //spark.sparkContext.addFile("transformer.py", true)
    val pipeRDD = arrayDFColumn.toJSON.rdd.pipe("./transformer.py")

    pipeRDD.toDF.withColumn("value", split(regexp_replace(col("value"), "[)(]", ""), ","))
      .select(col("value").getItem(0).as("uid"), col("value").getItem(1).as($(outputCol)))
  }
}