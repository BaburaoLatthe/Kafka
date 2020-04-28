package com.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerWithCallBack {

	private static Logger logger = LoggerFactory.getLogger(ProducerWithCallBack.class);
	
	public static void main(String[] args) {
		String localhost = "localhost:9092";

		// Properties for Producer
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, localhost);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// Create Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		// Send Data
		for(int i = 0 ; i<=9; i++) {
			ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("sports-channel", "Producer message from Java Programme :: " +i);
			
			producer.send(producerRecord, new Callback() {
				
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					
					if(exception == null) {
						logger.debug("Kafka Producer for topic " + metadata.topic() + " -- partition " + metadata.partition() + "-- offset "+ metadata.offset() + " \n ");
					}
					else {
						exception.printStackTrace();
					}
				}
			});
		}
		
		// flush & close producer
		producer.flush();
		producer.close();
	}
}
