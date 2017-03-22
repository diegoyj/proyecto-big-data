package es.operation

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor, TableName, HColumnDescriptor }
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

object PrepareData {
  
  @transient lazy val hbaseConn = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "quickstart")
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    conf.set("zookeeper.znode.parent", "/hbase");
    ConnectionFactory.createConnection(conf);
  }
  
  def main(args: Array[String]) {
    
    val sconf = new SparkConf().setAppName("Word Count").setMaster("local[*]")
    val sc = new SparkContext(sconf)
    
    val RDDUsuarios = sc.textFile("hdfs://localhost/master/pragsis/recomendador/raw/usuarios/*").filter(!_.isEmpty()).filter(user => user.split("\t").size==5)
    val RDDBanners = sc.textFile("hdfs://localhost/master/pragsis/recomendador/raw/banners/*").filter(!_.isEmpty())
    
    val bannersBc = RDDBanners.collect.toSet

    val RDDUserBaner = RDDUsuarios.map(user => (user.split("\t")(0), filterBanner(bannersBc, user)))
    
    //Guardar los calculos en HDFS, capa CLEAN
    val timestamp = System.currentTimeMillis()
    RDDUsuarios.saveAsTextFile("hdfs://localhost/master/pragsis/recomendador/clean/usuarios/"+timestamp)
    RDDUserBaner.saveAsTextFile("hdfs://localhost/master/pragsis/recomendador/clean/userBaner/"+timestamp)
    
    //Importar tabla a HBASE
    val tableName = "usersBannerTable"
    
    //CREATE TABLE
    val admin = hbaseConn.getAdmin();
    if(!admin.isTableAvailable(TableName.valueOf(tableName))) {
      val htd = new HTableDescriptor(tableName)
      val hcd = new HColumnDescriptor("cf1".getBytes())
      htd.addFamily(hcd)
      admin.createTable(htd)
    }
    
    //ADD DATA TO HBASE TABLE
    RDDUserBaner.foreachPartition{iter => 
      iter.foreach { x => 
        // esto ejecuta en el ejecutor
        val table = hbaseConn.getTable(TableName.valueOf(tableName))
        var put = new Put(x._1.getBytes())
        put.addColumn("cf1".getBytes(), "banner".getBytes(), x._2.getBytes())
        put.addColumn("cf1".getBytes(), "plays".getBytes(), "0".getBytes())
        table.put(put)
        // esto ejecuta en el ejecutor
      }}
    
    sc.stop()
  }
  
  def filterBanner(banners:Set[String], usuario:String): String = {
        val campos = usuario.split("\t")
        if(campos.size == 5) {
            val idUser = campos(0)
            val gender = campos(1)
            val age = campos(2)
            val date = campos(3)
            //Filter By Gender
            val ban = banners.filter(banner => banner.split("\t")(2)==gender.toUpperCase)
            if(age != "") {
                val banOK = ban.filter(banner => ((banner.split("\t")(0)!="") && (banner.split("\t")(1)!="") && (banner.split("\t")(0).toInt<age.toInt) && (banner.split("\t")(1).toInt>=age.toInt))).map(line => (line.split("\t")(3))).mkString("")
                (banOK)
            } else {
                val banOK = ban.filter(banner => ((banner.split("\t")(0)==age) && (banner.split("\t")(1)==age))).map(line => (line.split("\t")(3))).mkString("")
                (banOK)
            }
        } else {
            ("Error en Usuario, tamaño: "+campos.size)    
        }
    }
  
}