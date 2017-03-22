package es.generator

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils

object CalculateModel {
  
  def main(args: Array[String]) {
    val sconf = new SparkConf
    sconf.setAppName("CleanData: Banners y Reproducciones")
    //sconf.set("spark.ui.port", "4141")
    //sconf.set("spark.master", "spark://quickstart.cloudera:7077")
    sconf.setMaster("local[*]")
    val sc = new SparkContext(sconf)
    
    //Obtenemos el dataset de entrenamiento
    val events = sc.textFile("hdfs://localhost/master/pragsis/recomendador/raw/dataToModel/usersha1-artmbid-artname-plays.tsv").filter(!_.isEmpty())
//    val events = sc.textFile("C:/Users/Angel Cámara/Desktop/Big Data World/dataSets/lastfm-dataset-360K/usersha1-artmbid-artname-plays.tsv").filter(!_.isEmpty())
    
    //Nº de Plays total por usuario y le asignamos un key
    val playsForUser = events.map(e => (e.split("\t")(0), e.split("\t")(3).trim.toInt)).reduceByKey((v1, v2) => v1+v2).zipWithIndex()
    
    
    //Map con las keys por usuario
    val IdUserAlias = playsForUser.map {
        case ((idUser, plays), index) => (idUser, index)
    }
    
    //Nº de plays por grupo de música
    val playsForGroup = events.map(e => (e.split("\t")(1), e.split("\t")(3).trim.toInt)).reduceByKey((v1, v2) => v1+v2).zipWithIndex()
    
    //Map con las keys por grupo de música
    val IdGrupAlias = playsForGroup.map {
        case ((idGroup, plays), index) => (idGroup, index)
    }
    
    val IdUserAliasAsMap = IdUserAlias.collectAsMap()
    val IdGrupAliasAsMap = IdGrupAlias.collectAsMap()
    val trainData = events.map { line => {
        val User = IdUserAliasAsMap.get(line.split("\t")(0))
        val Group = IdGrupAliasAsMap.get(line.split("\t")(1))
        Rating(User.mkString("").toInt, Group.mkString("").toInt, line.split("\t")(3).trim.toInt)
    }}
    
    //Guardamos los alias en HDFS para el Spark Streaming de recomendaciones
    //Transofrmamos cada registro (alias) en un string separado por comas para borrar los parentesis
    IdUserAlias.map(alias => alias._1+ "," + alias._2).saveAsTextFile("hdfs://localhost/master/pragsis/recomendador/clean/datastreaming/aliasusuer-")
    IdGrupAlias.map(alias => alias._1+ "," + alias._2).saveAsTextFile("hdfs://localhost/master/pragsis/recomendador/clean/datastreaming/aliasgroups-")
    
    //Split RDDs
    val splitRDD = trainData.randomSplit(Array(0.6, 0.2, 0.2), seed=0L)
    val (trainingData, validationData, testData) = (splitRDD(0), splitRDD(1), splitRDD(2))
    val numValidation = validationData.count()
    val numTest = testData.count()
    
    //Calculate model and compare the best model
    val ranks = List(8, 12)
//    val lambdas = List(0.1, 10.0)
    val numIters = List(10, 20)
    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1
    
    for (rank <- ranks; numIter <- numIters) {
        val model = ALS.trainImplicit(trainingData, rank, numIter)

         val validationRmse = computeRmse(model, validationData, numValidation)
        println("RMSE (validation) = " + validationRmse + " for the model trained with rank = " + rank + ", and numIter = " + numIter + ".")
        if (validationRmse < bestValidationRmse) {
            bestModel = Some(model)
            bestValidationRmse = validationRmse
            bestRank = rank
//            bestLambda = lambda
            bestNumIter = numIter
        }
    }
    
    //Evaluamos el mejor modelo con el RDD Test
    val testRmse = computeRmse(bestModel.get, testData, numTest)
    println("The best model was trained with rank = " + bestRank + " and lambda = " + bestLambda
          + ", and numIter = " + bestNumIter + ", and its RMSE on the test set is " + testRmse + ".")
    
    
    /**
     * Guardar el modelo en HDFS.    
     */
//     model.save(sc, "hdfs://localhost/master/pragsis/recomendador/clean/modelRecomendation")
     
     sc.stop()
  }
  
   def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long): Double = {
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
      .join(data.map(x => ((x.user, x.product), x.rating)))
      .values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
  }
}