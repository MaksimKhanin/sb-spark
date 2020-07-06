package org.sberbank.lab07s

import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.ml.linalg.SQLDataTypes
import java.io.PrintWriter


trait SklearnEstimatorParams extends Params {
  final val inputCol= new Param[String](this, "inputCol", "The input column")
  final val outputCol = new Param[String](this, "outputCol", "The output column")
  final val vocabSize = new Param[Int](this, "vocabSize", "vocabluarySize")
}

trait HasSklearnModel extends Params {
  final val sklearn_model = new Param[String](this, "sklearn_model", "Parameter that contains a serizlized sklearn model")
  final val model_path = new Param[String](this, "model_path", "Parameter that contains model path")
}

class SklearnEstimator(override val uid: String) extends Estimator[SklearnEstimatorModel]
  with HasSklearnModel
  with SklearnEstimatorParams{

  def setInputCol(value: String) = set(inputCol, value)

  def setOutputCol(value: String) = set(outputCol, value)

  def setVocabSize(value: Int) = set(vocabSize, value)

  def setModelPath(value: String) = set(model_path, value)

  def this() = this(Identifiable.randomUID("sklearnestimator"))

  override def copy(extra: ParamMap): SklearnEstimator = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    //val requiredSchema = StructField($(inputCol), ArrayType(DataTypes.LongType))
    val requiredSchema = StructField($(inputCol), SQLDataTypes.VectorType, false)
    val actualDataType = schema($(inputCol))
    require(actualDataType.equals(requiredSchema),
      s"Schema ${$(inputCol)} must be $requiredSchema but provided $actualDataType")
    schema.add(StructField($(outputCol), StringType, false))
  }

  override def fit(dataset: Dataset[_]): SklearnEstimatorModel = {

    val toDense = udf((v:MLVector) => v.toDense.toArray)
    val df = dataset.withColumn($(inputCol), toDense(col($(inputCol))))
      .select("uid", $(inputCol), "gender_age")

    val arrayDFColumn = df.select(
      df("uid") +: df("gender_age") +: (0 until $(vocabSize)).map(i => df($(inputCol))(i).alias(s"feature_$i")): _*
    )

    //spark.sparkContext.addFile("fit.py", true)
    val pipeRDD = arrayDFColumn.toJSON.rdd.pipe("./fit.py")
    val model = pipeRDD.collect.head
    set(sklearn_model, model)
    new PrintWriter($(model_path)) { write(pipeRDD.collect.head); close() }
    val newEstimator = new SklearnEstimatorModel(uid)
    newEstimator.set(inputCol, $(inputCol))
      .set(outputCol, $(outputCol))
      .set(sklearn_model, $(sklearn_model))
      .set(vocabSize, $(vocabSize))
      .set(model_path, $(model_path))
  }
}
