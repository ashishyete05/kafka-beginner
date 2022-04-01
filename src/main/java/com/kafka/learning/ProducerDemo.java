package com.kafka.learning;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {

	public static void main(String[] args) {

		// properties for kafka

		final String bootstrapServersList = "localhost:9092";
		final String keySerializer = StringSerializer.class.getName();
		final String valueSerializer = StringSerializer.class.getName();

		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersList);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);

		// KafkaProducer Object

		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

		// Producer Record to send data to a topic

		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("first_topic","ABCDEFG!!!!!!!");
		kafkaProducer.send(producerRecord);
		System.out.println("Kafka message Sent.");
		// close the kafka producer.
		kafkaProducer.flush();
		kafkaProducer.close();

	}

}
