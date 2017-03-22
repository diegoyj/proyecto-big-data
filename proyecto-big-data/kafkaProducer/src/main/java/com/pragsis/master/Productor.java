package com.pragsis.master;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

public class Productor {
	 final static Logger logger = Logger.getLogger(Productor.class);

	 public static void main(String[] args) {
			
	     Properties props = new Properties();
	     //Servidor de Kafka
	     props.put("bootstrap.servers", "localhost:9092");
	     props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");	    	      
	     props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
	     //props.put("partitioner.class","kafka.CustomPartitioner");

	     String topic = "ineventos";
	     
	    //Parametros para la lectura de fichero
	    File dir = new File("/home/cloudera/Desktop/DataSets/lastfm-dataset-1K");
	    Path path = null;
	    Charset charset = Charset.forName("UTF-8");

	    try {
			path = Paths.get(dir.getCanonicalPath(), "userid-timestamp-artid-artname-traid-traname.tsv");
		} catch (IOException e1) {
			e1.printStackTrace();
		}

	    //Crear el productor de Kafka
 	    KafkaProducer<String, String> productor = new KafkaProducer<String, String>(props);
 
		//Leer fichero
		try (BufferedReader reader = Files.newBufferedReader(path, charset)) {
		    String event, userId, timestamp, artistId, artistName, trackId, trackName, key, value;
		    event = userId = timestamp = artistId= artistName = trackId = trackName = key = value = null;
		    
		    while ((event = reader.readLine()) != null) {
		    	String[] event_parts = event.split("\t", -1);
		    	//Si no tenemos un con seis campos, lo descartamos.		    	
		    	if(event_parts.length == 6){
			    	userId = event_parts[0] + "\t";
			    	timestamp = event_parts[1] + "\t";
			    	artistId = event_parts[2] + "\t";
			    	artistName = event_parts[3] + "\t";
			    	trackId = event_parts[4] + "\t";
			    	trackName = event_parts[5] + "\t";
			    	key = userId;
			    	value = userId.concat(timestamp).concat(artistId).concat(artistName).concat(trackId).concat(trackName);

			   	 	//Enviar evento a la cola de kafka. 
		        	productor.send(new ProducerRecord<String, String>(topic,key,value));
		        	logger.debug("Evento añadido en el topic" + topic +  "\nEvento: Key=" + key + "Value: " + value);
		        	Thread.sleep(1000);
		    	}
		    }
		} catch (IOException x) {
		    System.err.format("IOException: %s%n", x);
		} catch (InterruptedException e) {
		    System.err.format("InterruptedException: %s%n", e);
			e.printStackTrace();
		}
	     productor.close();	      
	 }
}