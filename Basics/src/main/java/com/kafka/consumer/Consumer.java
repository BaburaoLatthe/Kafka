package com.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafka.constant.Constants;

public class Consumer {

	private static Logger logger = LoggerFactory.getLogger(Consumer.class);

	private static String GROUP_ID = "consumer_group";
	private static Collection<String> topic_List = Arrays.asList("sports_channel");

	private static int count;

	public static void main(String[] args) {

		// Create consumer config
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BOOTSTRAP_SERVER);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "1000");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

		int numberOfRecordsRead = 0;
		Boolean keepReading = true;
		try {
			consumer.subscribe(topic_List);
			while (keepReading) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				count = records.count();
				for (ConsumerRecord<String, String> record : records) {
					logger.info(String.format("offset = %d, key = %s, value = %s%n", record.offset(), record.key(),
							record.value()));

					if (numberOfRecordsRead == count) {
						keepReading = false;
						break;
					}
					numberOfRecordsRead++;
				}
			}
		} catch (KafkaException ex) {
			logger.error("Error occured because of KafkaConsumer ");
		} finally {
			consumer.close();
		}

	}

}
