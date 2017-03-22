package com.pragsis.master

import util.Random
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}

object MapeoDatos {

case class Fila(userId: String,  artistId: String, artistName: String, plays: Int)

def main(args: Array[String]) {
	val sconf = new SparkConf()
	.setAppName("mapearusuarios")
	.setMaster("local[*]")
	val sc = new SparkContext(sconf)

	//Get train dataset  <userId, artistId, artistName, plays>
	val events = sc.textFile("hdfs://localhost/master/pragsis/recomendador/raw/datatomodel/usersha1-artmbid-artname-plays.tsv")

	// SPARKSQL. Crear un contexto de HiveSql porque proporciona más funcionalidades en 1.6
	//val sqlContext = new org.apache.spark.sql.SQLContext(sc)
	val sqlContext =  new org.apache.spark.sql.hive.HiveContext(sc)

	// this is used to implicitly convert an RDD to a DataFrame.
	import sqlContext.implicits._

	// Crear dataframe y registrarlo como tabla
	val dataset = sc
	.textFile("hdfs://localhost/master/pragsis/recomendador/raw/datatomodel/usersha1-artmbid-artname-plays.tsv")
	.map(_.split("\t"))
	.map(fields => Fila(fields(0),fields(1), fields(2), fields(3).trim.toInt))
	.toDF()
	.registerTempTable("eventos")

	val allUsers  = sqlContext.sql("SELECT DISTINCT(userId) from eventos")
	.registerTempTable("allUsers")

	val allArtist  = sqlContext.sql("SELECT DISTINCT(artistId) from eventos")
	.registerTempTable("allArtist")

	val idsMapeadosUsuarios = sqlContext.sql("SELECT userId, row_number() OVER () as rank FROM allUsers")
	val idsMapeadosArtistas = sqlContext.sql("SELECT artistId, row_number() OVER () as rank FROM allArtist")

	//Print Results
	idsMapeadosUsuarios.map({row => "UserId : " + row(0) + ". New Id: " + row(1)}).take(10).foreach(println)
	idsMapeadosArtistas.map({row => "ArtistId : " + row(0) + ". New Id: " + row(1)}).take(10).foreach(println)

	// Delete the existing path, ignore any exceptions thrown if the path doesn't exist
//	val hadoopConf = new org.apache.hadoop.conf.Configuration()
//	val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://localhost:9000"), hadoopConf)
//	val output = "hdfs://localhost:9000/master/pragsis/recomendador/clean/mapeo-usuarios"
//	try { hdfs.delete(new org.apache.hadoop.fs.Path(output), true); System.out.println("BORRADO") } catch { case _ : Throwable => {println } }

	//Save user mapper to hdfs
	idsMapeadosUsuarios.write
	.format("com.databricks.spark.csv")
	.option("header", "false")
	.option("delimiter", "\t")
  .save("hdfs://localhost/master/pragsis/recomendador/clean/mapeo-usuarios")

  //Save artist mapper to hdfs
  	idsMapeadosArtistas.write
	.format("com.databricks.spark.csv")
	.option("header", "false")
	.option("delimiter", "\t")
  .save("hdfs://localhost/master/pragsis/recomendador/clean/mapeo-artistas")

}  

}
