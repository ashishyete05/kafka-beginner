package com.kafka.learning;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class ConsumerDemo {
	
	final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

	public static void main(String[] args) {

		final String BOOTSTRAP_SERVERS = "localhost:9092";
		final String TOPIC_NAME = "first_topic";
		final String GROUP_NAME = "group-1";
		final String KEY_DESERIALIZER = StringDeserializer.class.getName();
		final String VALUE_DESERIALIZER = StringDeserializer.class.getName();

		// properties
		Properties properties = new Properties();
		properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, KEY_DESERIALIZER);
		properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER);
		properties.setProperty(GROUP_ID_CONFIG, GROUP_NAME);
		properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");

		// kafkaconsumer
		KafkaConsumer<String, String> kafkaConsumer = null;
		try {
			kafkaConsumer = new KafkaConsumer<String, String>(properties);
			kafkaConsumer.subscribe(Arrays.asList(TOPIC_NAME));

			while (true) {

				ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord<String, String> cr : consumerRecords) {
					System.out.println("Message received > " + cr.value());
				}
			}
		} catch (Exception ex) {
			System.out.println(ex.getMessage());
		} finally {
			kafkaConsumer.close();
		}

	}

}
