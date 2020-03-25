package org.twitter.com;

import java.util.Arrays;
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

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class ProdCon {
	private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
	private static final String CONSUMER_KEY = "D0lGy4A14IMewfyOETs1LtkRo";
	private static final String CONSUMER_SECRET = "xiiq3E0abcNMVkp9klTsgF5u05Kq21Y4rAEjOBW39HhSs8jMi5";
	private static final String TOKEN = "1242070904303161344-nqohtvyGRZFZYEp5Ok3DetQEKcP4Pe";
	private static final String SECRET = "8PCtJKglw7aSQKmfUZ2Kc8Sk0gundBH2MNEU68yTcR1AS";
	Logger logger = LoggerFactory.getLogger(ProdCon.class);

	public static void main(String[] args) {
		new ProdCon().run();

	}

	private void run() {
		BlockingQueue<String> msMessage = new LinkedBlockingQueue<String>(100000);
		KafkaProducer<String, String> producer = createProducer();
		Client client = createClient(msMessage);

		client.connect();
		String msg = null;
		while (!client.isDone()) {

			try {
				msg = msMessage.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				logger.info(e.getMessage());
				client.stop();
			}
			if (msg != null) {
				producer.send(new ProducerRecord<String, String>("twitter", null, msg), new Callback() {

					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						if (exception != null) {
							logger.info("an error has accured");
						}

					}
				});
			}
			
		}
		logger.info("the end");
	}

	private KafkaProducer<String, String> createProducer() {
		Properties prop = new Properties();
		prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
		return producer;
	}

	private Client createClient(BlockingQueue<String> msMessage) {
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		endpoint.trackTerms(Arrays.asList("trump"));
		return new ClientBuilder().name("self").hosts(new HttpHosts(Constants.STREAM_HOST))
				.authentication(new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET)).endpoint(endpoint)
				.processor(new StringDelimitedProcessor(msMessage)).build();
	}

}
