package com.bigdata.streaming.example.util

import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame

object ModelUtil {

  def addPredictions(inputDF: DataFrame, model: KMeansModel) : DataFrame = {
    val featureCols = Array("lat", "lon")
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val featureDF = assembler.transform(inputDF)
    val predictionsDF = model.transform(featureDF)
    predictionsDF
  }
}
