package es.recomendador

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import java.util.HashMap
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.dmg.pmml.NearestNeighborModel


object SuggestGroup {
  
  @transient lazy val props = {
    val props = new HashMap[String, Object]()
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
//    new KafkaProducer[String, String](props)
    props
  }
  
  def main(args: Array[String]) {
    //Streaming config
		val conf = new SparkConf().setMaster("local[*]").setAppName("CheckEvent")
		val ssc = new StreamingContext(conf, Seconds(2))
		
		//Kafka config
		val topics = Set("ineventos")
		val kafkaParams: Map[String, String] = Map("metadata.broker.list" -> "localhost:9092")

		//Read from Kafka. Read. Filter. Map. ForEach 
		val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
		

		
		//cache or presist rdds with alias
		val infoGrupos = ssc.sparkContext.textFile("hdfs://localhost/master/pragsis/recomendador/clean/datastreaming/aliasgroups-").map(line => (line.split("\t")(0), line.split("\t")(1), line.split("\t")(2)))
		val ids = infoGrupos.map {
		  case(idGrupo, indice, nombre) => (idGrupo, indice)
		}//(idGrupo, uniqueId) CHECKPOINT!!!!
		
		val nombres = infoGrupos.map{
		  case(idGrupo, indice, nombre) => (indice, nombre)
		}//(idGrupo, nombres) CHECKPOINT!!!!!
		
		val idsGruposMap = ids.collectAsMap()
		val nombresMap = nombres.collectAsMap()
		
		
		val modelInHDFS = MatrixFactorizationModel.load(ssc.sparkContext, "hdfs://localhost/master/pragsis/recomendador/clean/modelrecomendation")
		
		
		val events = kafkaStream.map(_._2).filter(x => !x.equals("")).map(line => (0, idsGruposMap.get(line.split("\t")(2)).mkString.toInt)).filter(y => !y._2.equals(""))

		
		//Escribir en kafka para las recomendaciones
		events.foreachRDD ( rdd => { 
		  val recomendaciones = modelInHDFS.predict(rdd).sortBy(- _.rating).take(1)///¿¿¿¿¿?????
		  
		  rdd.foreachPartition(partition => {
		    val producer = new KafkaProducer[String, String](props)
		    
		    recomendaciones.foreach { record =>
		    val grupoRecomendado = "GRUPO RECOMENDADO >>>>> "+nombresMap.get(record.product.toString())
		    
		    producer.send(new ProducerRecord("outrecomendacion", null, grupoRecomendado.mkString))
		  }
		  })
		  
		})
		  
		  
		  




		// after setup, start the stream
    ssc.start()
    ssc.awaitTermination()
  }
  
}