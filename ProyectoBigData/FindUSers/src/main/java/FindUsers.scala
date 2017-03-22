package com.pragsis.master

import util.Random
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFLead.LeadBuffer
import sun.security.util.Length

object FindUsers {

case class Fila(id: String,  gender: String, age: String, country: String, registered:String)

def main(args: Array[String]) {
	val sconf = new SparkConf()
	.setAppName("mapearusuarios")
	.setMaster("local[*]")
	val sc = new SparkContext(sconf)

	//Get train dataset  <userId, gender, age, country, registered>
//	val user1k = sc.textFile("hdfs://localhost/master/pragsis/recomendador/raw/userinfo/usersha1-profile1k.tsv")
//	val user360k = sc.textFile("hdfs://localhost/master/pragsis/recomendador/raw/userinfo/usersha1-profile360k.tsv")

	
	// SPARKSQL. Crear un contexto de HiveSql porque proporciona más funcionalidades en 1.6
	val sqlContext =  new org.apache.spark.sql.hive.HiveContext(sc)
	import sqlContext.implicits._

	// Crear dataframe y registrarlo como tabla
	val users1k = sc
	.textFile("hdfs://localhost/master/pragsis/recomendador/raw/userinfo/userid-profile.tsv")
	.filter(_.split("\t").length > 1) //elimina los usuarios que no tienen datos
	.map(_.split("\t"))
	.map(fields =>{
	  val v = if (fields(0).isEmpty() || (fields(0) == null)) "" else fields(0)
	  val w = if (fields(1).isEmpty() || (fields(1) == null)) "" else fields(1)
	  val x = if (fields(2).isEmpty() || (fields(2) == null)) "" else fields(2)
	  val y = if (fields(3).isEmpty() || (fields(3) == null)) "" else fields(3)
	  val z = if (fields(4).isEmpty() || (fields(4) == null)) "" else fields(4)
	  Fila(v,w,x,y,z)
	})
	.toDF()
	
	val users360k = sc
	.textFile("hdfs://localhost/master/pragsis/recomendador/raw/userinfo/usersha1-profile.tsv")
	 .filter(_.split("\t").length > 1) //elimina los usuarios que no tienen datos
	.map(_.split("\t"))
	.map(fields => Fila(fields(0), fields(1), fields(2), fields(3), fields(4)))
  .toDF()
	
	val table1k = users1k.registerTempTable("users1k")
	val table360k = users360k.registerTempTable("users360k")

  val query = sqlContext.sql("SELECT * FROM users1k JOIN users360k " + 
                             "ON  users1k.registered = users360k.registered " +
                             "AND users1k.country = users360k.country " + 
                             "AND users1k.gender = users360k.gender " + 
                             "AND users1k.age = users360k.age ")//.show()

	//Print Results
	System.out.println("size: " + query.collect.length)
	query.take(50).foreach { println }
}  

}
