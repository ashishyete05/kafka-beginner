package com.kafka.learning;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallBack {

	final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);

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

		for (int i=1;i<11;i++) {
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("first_topic","Producer Demo will call Back "+i + " times");
		kafkaProducer.send(producerRecord, new Callback() {
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if(exception!=null) {
					System.out.println("Exception Caught : \n "+exception.getMessage());
				}else {
					System.out.println("New MetaData Received :"
							+ "\n Topic : "+metadata.topic()
							+ "\n Offset : "+metadata.offset()
							+ "\n TimeStamp : "+metadata.timestamp());
				}
			}
		});
		}
		
		
		System.out.println("Kafka message Sent.");
		// close the kafka producer.
		kafkaProducer.flush();
		kafkaProducer.close();

	}

}
