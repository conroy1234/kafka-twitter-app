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

public class CreatedTweeksSelf {
	Logger logger = LoggerFactory.getLogger(CreatedTweeksSelf .class);
	private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
	private static final String CONSUMER_KEY = "D0lGy4A14IMewfyOETs1LtkRo";
	private static final String CONSUMER_SECRET = "xiiq3E0abcNMVkp9klTsgF5u05Kq21Y4rAEjOBW39HhSs8jMi5";
	private static final String TOKEN = "1242070904303161344-nqohtvyGRZFZYEp5Ok3DetQEKcP4Pe";
	private static final String SECRET = "8PCtJKglw7aSQKmfUZ2Kc8Sk0gundBH2MNEU68yTcR1AS";

	public static void main(String[] args) {
		new CreatedTweeksSelf().run();

	}

	private void run() {
		BlockingQueue<String> mqMessage = new LinkedBlockingQueue<String>(100000);
		Client client = createClient(mqMessage);
		
		client.connect();
		
		KafkaProducer<String, String> producer = createProducer();
		String msg=null;
		while(!client.isDone()) {
			try {
				 msg = mqMessage.poll(5,TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				
				e.printStackTrace();
				client.stop();
			}
			if(msg!=null) {
				logger.info(msg);
				producer.send(new ProducerRecord<String, String>("twitter", null,msg),new Callback() {
					
					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
					if(exception != null) {
						logger.info("something bad has happened");
					}
						
					}
				});
			}
		}
	}

	private KafkaProducer<String, String> createProducer() {
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		KafkaProducer<String, String>  producer = new KafkaProducer<String, String>(properties);
		return producer;
	}

	public Client createClient(BlockingQueue<String> mqMessage) {
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		endpoint.trackTerms(Arrays.asList("forex"));
		return new ClientBuilder().name("selfassess")
				.name("self-created")
				.hosts(new HttpHosts(Constants.STREAM_HOST))
				.endpoint(endpoint)
				.authentication(new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET))
				.processor(new StringDelimitedProcessor(mqMessage))
				.build();
	}

}
