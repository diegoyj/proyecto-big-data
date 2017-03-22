package com.pragsis.master

//Util
import java.util.HashMap
//Streaming
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
//Spark
import org.apache.spark.SparkConf
//HBase
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor, TableName, HColumnDescriptor }
//Kafka
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}


object ReadAndCheckEvent {

	@transient lazy val connHbase = {
			val conf = HBaseConfiguration.create()
					conf.set("hbase.zookeeper.quorum", "quickstart.cloudera")
					conf.set("hbase.zookeeper.property.clientPort", "2181")
					conf.set("zookeeper.znode.parent", "/hbase")
					ConnectionFactory.createConnection(conf);
	}

	var NumSongs = 10

			def main(args: Array[String]) {

		//Streaming config
		val conf = new SparkConf().setMaster("local[*]").setAppName("CheckEvent")
				val ssc = new StreamingContext(conf, Seconds(2))

		//Kafka config
		val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")
		val topics = List("ineventos").toSet
		val kafkaBrokers = "localhost:9092"

		//Hbase config. https://autofei.wordpress.com/2012/04/02/java-example-code-using-hbase-data-model-operations/
		val tableName = "usersBannerTable"
		val hbaseColumnName = "cf1"
		val qualifier = "plays"

		//Read from Kafka. Read. Filter. Map. ForEach 
		val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

		//Aplicar la función a cada usuario que leamos del streaming
		kafkaStream
		.map(_._2)
		.filter(line => line.split("\n")(0) != "")
		.map(line => (line.split("\t")(0),1))
		.foreachRDD(rdd =>{
		  System.out.println("# events = " + rdd.count())
		  
		  rdd.foreachPartition(partition => {
  		    // Print statements in this section are shown in the executor's stdout logs
  		    // Asi se crea un productor por cada worker
      		val props = new HashMap[String, Object]()
      		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
      		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      				"org.apache.kafka.common.serialization.StringSerializer")
   				props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      				"org.apache.kafka.common.serialization.StringSerializer")
      				
  		    val producer = new KafkaProducer[String, String](props)
  		    partition.foreach(record => {
  		         sumPlays(record._1, tableName, hbaseColumnName,qualifier, producer)
  		    })
		  })
		})

		ssc.start()
		ssc.awaitTermination()
	}

	def sumPlays(userId:String, tableName:String, columnName:String, qualifier:String, producer:KafkaProducer[String, String] ) = {
		//Obtener la tabla
		connHbase.getTable(TableName.valueOf(tableName))
		val table = connHbase.getTable(TableName.valueOf(tableName))

		// Obtener el numero de plays para el userId
		val get = new Get(userId.getBytes());
		val result = table.get(get);
		val playsbytes = result.getValue("cf1".getBytes(), "plays".getBytes());
		val banner = result.getValue("cf1".getBytes(), "banner".getBytes());
		val plays = Bytes.toString(playsbytes)

		System.out.println("User: " + userId + " . Plays: " + plays + " Banner: " + banner );

		//sumar +1 al contador de plays en Hbase
		val put = new Put(userId.getBytes())
		put.addColumn(columnName.getBytes(), qualifier.getBytes(), (plays.toInt + 1).toString().getBytes())
		table.put(put)

		//Comprobar publicidad. Sumar +1 para contar la reproducción actual.
		if((plays.toInt + 1) % NumSongs == 0){
			sendPubliToKafka("Usuario: " + userId + ". Banner: " + banner, producer)
			System.out.println("Publicidad enviada al usuario: " + userId)
		}
	}

	def sendPubliToKafka(userId:String, producer:KafkaProducer[String, String]) = {
    val kafkaTopic = "outpublicidad"
		val message = new ProducerRecord[String, String](kafkaTopic, null, userId)     
		producer.send(message)
	}
}

