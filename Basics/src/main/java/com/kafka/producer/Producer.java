package com.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafka.constant.Constants;

public class Producer {
	private static Logger logger = LoggerFactory.getLogger(Producer.class);
	private static final String KAFKA_TOPIC = "sports-channel";
 
	public static void main(String[] args) {

		// Properties for Producer
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BOOTSTRAP_SERVER);
		properties.setProperty(ProducerConfig.ACKS_CONFIG, Constants.ACKS_CONFIG);  // The "all" setting we have specified will result in blocking on the full commit of the record
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, Constants.RETRIES_CONFIG);  // If the request fails, the producer can automatically retry, though since we have specified retries as 0 it won't. 
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");    // The producer maintains buffers of unsent records for each partition. 
		properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");  // The buffer.memory controls the total amount of memory available to the producer for buffering.
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1"); // if you want to reduce the number of requests you can set linger.ms to something greater than 0.
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// Create Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		// Send Data
		for(int i = 0 ; i<9 ; i++) {
			ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(KAFKA_TOPIC, "Baburao", "Producer message from Java Programme :: " +i);
			producer.send(producerRecord);
			logger.info(String.format("\n partition = %d, key = %s, value = %s%n", producerRecord.partition(), producerRecord.key(),
					producerRecord.value()));
		}
		
		// flush & close producer
		producer.flush();
		producer.close();
	}
}
