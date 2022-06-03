package com.kafka.twitter;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {
	final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
	public Client client = null;
	KafkaProducer<String, String> kafkaProducer =null;
	public TwitterProducer() {
	}

	public static void main(String[] args) {
		new TwitterProducer().run();
	}
	
	public void run() {
		logger.info("Twitter producer Started");
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
		
		client = createTwitterClient(msgQueue);
		client.connect();
		
		Runtime.getRuntime().addShutdownHook(new Thread(()->{
			logger.info("Shutting down Application");
			logger.info("Stopping Client");
			client.stop();
			logger.info("Closing Kafka Producer");
			kafkaProducer.close();
			logger.info("Application shutdown Gracefully!");
		}));
		
		kafkaProducer = createKafkaProducer();
		
		
		while (!client.isDone()) {
			String msg =null;
			try {
				 msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException ex) {
				ex.printStackTrace();
				client.stop();
			}
			if(msg!=null) {
				logger.info("twitter message > "+msg);
				kafkaProducer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg), new Callback() {
					
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						if(exception!=null) {
							logger.error("Exception Caught" +exception.getMessage());
						}
					}
				});
			}
		}
		logger.info("Twitter producer Stopped");
		
	}
	
	

	private KafkaProducer<String, String> createKafkaProducer() {

		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//safe producer
		properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		properties.put(ProducerConfig.ACKS_CONFIG, "all");
		properties.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
		//high throughput 
		properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		properties.put(ProducerConfig.LINGER_MS_CONFIG, "20");
		properties.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));

		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
		return kafkaProducer;
	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {

		


		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		List<String> terms = Lists.newArrayList("India","cricket","politics");
		hosebirdEndpoint.trackTerms(terms);

		Authentication hosebirdAuth = new OAuth1(consumerKey,consumerSecret,token,secret);

		//Creating a client
		ClientBuilder builder = new ClientBuilder()
				  .name("Hosebird-Client-01")                              // optional: mainly for the logs
				  .hosts(hosebirdHosts)
				  .authentication(hosebirdAuth)
				  .endpoint(hosebirdEndpoint)
				  .processor(new StringDelimitedProcessor(msgQueue));

				Client hosebirdClient = builder.build();
				return hosebirdClient;
		
	}
}
