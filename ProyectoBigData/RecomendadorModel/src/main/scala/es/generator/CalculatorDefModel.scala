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
//import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, RowMatrix}

object CalculatorDefModel {
   def main(args: Array[String]) {
    val sconf = new SparkConf
    sconf.setAppName("CleanData: Banners y Reproducciones")
    //sconf.set("spark.ui.port", "4141")
    //sconf.set("spark.master", "spark://quickstart.cloudera:7077")
    sconf.setMaster("local[*]")
    val sc = new SparkContext(sconf)
    
    //Obtenemos el dataset de entrenamiento
    val events = sc.textFile("hdfs://localhost/master/pragsis/recomendador/raw/datatomodel/usersha1-artmbid-artname-plays.tsv")
    
    //Nº de Plays total por usuario y le asignamos un key
    val playsForUser = events.map(e => (e.split("\t")(0), e.split("\t")(3).trim.toInt)).reduceByKey((v1, v2) => v1+v2).zipWithUniqueId()
    
    
    //Map con las keys por usuario
    val IdUserAlias = playsForUser.map {
        case ((idUser, plays), index) => (idUser, index)
    }
    
    
    
    
    
    //Nº de plays por grupo de música
    val playsForGroup = events.map(e => (e.split("\t")(1), e.split("\t")(3).trim.toInt)).reduceByKey((v1, v2) => v1+v2).zipWithUniqueId()
    
    //Map con las keys por grupo de música
    val IdGrupAlias = playsForGroup.map {
        case ((idGroup, plays), index) => (idGroup, index)
    }
    
    val pairGroup = events.map(e => (e.split("\t")(1), e.split("\t")(2))).distinct()
    val infoGroup = IdGrupAlias.join(pairGroup).map {
      case (idGrupo, (nombre, index)) => (idGrupo, nombre, index)
    }.map(line => line._1+"\t"+line._2+"\t"+line._3)
    
    infoGroup.saveAsTextFile("hdfs://localhost/master/pragsis/recomendador/clean/datastreaming/aliasgroups-")
    
    
    
    
    
    //Preparamos los datos para el entrenamiento
    val IdUserAliasAsMap = IdUserAlias.collectAsMap()
    val IdGrupAliasAsMap = IdGrupAlias.collectAsMap()
    val trainData = events.map { line => {
        val User = IdUserAliasAsMap.get(line.split("\t")(0))
        val Group = IdGrupAliasAsMap.get(line.split("\t")(1))
        Rating(User.mkString("").toInt, Group.mkString("").toInt, line.split("\t")(3).trim.toInt)
    }}
  
    
    val rank = 12
    val numIter = 20
    var bestModel: Option[MatrixFactorizationModel] = None
    
    /**
     * CALCULATE NEW MODEL
     */
    val model = ALS.trainImplicit(trainData, rank, numIter)
    bestModel = Some(model)
    
     
    /**
     * Guardar el modelo en HDFS.    
     */
     model.save(sc, "hdfs://localhost/master/pragsis/recomendador/clean/modelrecomendation")
     
     sc.stop()
  }
  
//   def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long): Double = {
//    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
//    val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
//      .join(data.map(x => ((x.user, x.product), x.rating)))
//      .values
//  }
}