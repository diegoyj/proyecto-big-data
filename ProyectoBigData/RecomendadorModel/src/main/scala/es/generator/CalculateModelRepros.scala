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

object CalculateModelRepros {
  
  def main(args: Array[String]) {
    val sconf = new SparkConf
    sconf.setAppName("CleanData: Banners y Reproducciones")
    //sconf.set("spark.ui.port", "4141")
    //sconf.set("spark.master", "spark://quickstart.cloudera:7077")
    sconf.setMaster("local[*]")
    val sc = new SparkContext(sconf)
    
    
//    val events = sc.textFile("hdfs://localhost/master/pragsis/recomendador/raw/datatomodelrepros/usersha1-artmbid-artname-plays.tsv")
//    .map(e => (e.split("\t")(0), e.split("\t")(2))).groupBy(idUser, idGroup)
//    
//    val userGroup = events.map{
//      case(idUser, idGroup) => ((idUser, idGroup), 1)
//    }
//    
//    userGroup.reduceByKey({
//      case (x, y) => (x._1 + y, x._2 + y)
//    })
    
    
    
  }
}