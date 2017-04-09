package com.saurzcode.twitter;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.log4j.BasicConfigurator;

/**
 *
 */
public class TwitterKafkaProducer {

	private static final String topic = "twitter-topic";

	/**
	 *
	 * @param consumerKey
	 * @param consumerSecret
	 * @param token
	 * @param secret
	 * @throws InterruptedException
	 */
	public static void run(String consumerKey, String consumerSecret,
			String token, String secret) throws InterruptedException {
		System.out.print("Configuring kafka producer... ");
		Properties properties = new Properties();
		properties.put("metadata.broker.list", "localhost:9092");
		properties.put("serializer.class", "kafka.serializer.StringEncoder");
		properties.put("client.id","camus");

		ProducerConfig producerConfig = new ProducerConfig(properties);
		Producer<String, String> producer =
				new Producer<String, String>(producerConfig);
		System.out.println(" done.");
		System.out.print("Configuring and starting twitter client... ");
		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		// add some track terms
		endpoint.trackTerms(
				Lists.newArrayList("twitterapi", "#AAPSweep", "#makeitcount"));

		Authentication auth = new OAuth1(consumerKey, consumerSecret, token,
				secret);
		// Authentication auth = new BasicAuth(username, password);

		// Create a new BasicClient. By default gzip is enabled.
		Client client = new ClientBuilder().hosts(Constants.STREAM_HOST)
				.endpoint(endpoint).authentication(auth)
				.processor(new StringDelimitedProcessor(queue)).build();

		// Establish a connection
		client.connect();
		System.out.println(" done.");
		System.out.println("Sending messages ... ");
		// Do whatever needs to be done with messages
		for (int msgRead = 0; msgRead < 100; msgRead++) {
			KeyedMessage<String, String> message = null;
			try {
				message = new KeyedMessage<String, String>(topic, queue.take());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			producer.send(message);
			System.out.println("Message ["+msgRead+"] sent. \nTopic: "
					+ message.topic() + ", Product prefix: "
					+ message.productPrefix() + ", \nmsg: "
					+ message.message());
		}
		producer.close();
		client.stop();
	}

	/**
	 *
	 * @param args
	 */
	public static void main(String[] args) {
//		BasicConfigurator.configure();
		if(args.length < 4) {
			System.out.println("Usage: java TwitterKafkaProducer consumerKey "
					+ "consumerSecret token secret");
			return;
		}
		try {
			TwitterKafkaProducer.run(args[0], args[1], args[2], args[3]);
		} catch (InterruptedException e) {
			System.out.println(e);
		}
	}
}
