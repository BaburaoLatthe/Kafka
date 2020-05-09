package com.kafka.consumer;


import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafka.constant.Constants;

/**
 * 
 * @author Baburao A Latthe
 * Consumer Assign & Seek will read topic details from only specific partition assigned and Seek will 
 * read from specific offset of the partition assinged
 *
 */

public class ConsumerAssingSeek {

	private static Logger logger = LoggerFactory.getLogger(ConsumerAssingSeek.class);

	public static void main(String[] args) {

		// Create consumer config
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BOOTSTRAP_SERVER);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

		try {
			// Assign to consumer
			Collection<TopicPartition> partitions = new ArrayList<TopicPartition>();
			int partitionNumber = 0;
			TopicPartition partition1 = new TopicPartition("sports_channel", partitionNumber);
			partitions.add(partition1);
			consumer.assign(partitions);

			// Seek partition from consumer
			long offset = 5L;
			consumer.seek(partition1, offset);

			int numberOfMessageToBeRead = 10;
			int numberOfMessage = 0;

			boolean keepReading = true;
			while (keepReading) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord<String, String> record : records) {
					numberOfMessage++;
					logger.info(String.format("\n offset = %d, key = %s, value = %s%n", record.offset(), record.key(),
							record.value()));

					if (numberOfMessage >= numberOfMessageToBeRead) {
						keepReading = false;
						break;
					}
				}
			}
		} catch (KafkaException ex) {
			logger.error("Error occured because of KafkaConsumer ");
		} finally {
			consumer.close();
		}

	}

}
