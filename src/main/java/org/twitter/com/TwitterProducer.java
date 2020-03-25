package org.twitter.com;

import java.util.Arrays;
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
	Logger logger = LoggerFactory.getLogger(TwitterProducer.class); 
private String CONSUMER_KEY="D0lGy4A14IMewfyOETs1LtkRo";
private String CONSUMER_SECRET="xiiq3E0abcNMVkp9klTsgF5u05Kq21Y4rAEjOBW39HhSs8jMi5";
private String TOKEN="1242070904303161344-nqohtvyGRZFZYEp5Ok3DetQEKcP4Pe";
private String SECRET="8PCtJKglw7aSQKmfUZ2Kc8Sk0gundBH2MNEU68yTcR1AS";
	public static void main(String[] args) {
		
		new TwitterProducer().run();
	}

	private void run() {
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
		Client client = createTwitterClient(msgQueue);
		client.connect();
		
		KafkaProducer<String, String> producer = createProducer();
		

		
		// on a different thread, or multiple different threads....
		while (!client.isDone()) {
			String msg=null;
		  try {
			msg = msgQueue.poll(5,TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			
			e.printStackTrace();
			client.stop();
		}
		 // something(msg);
		 // profit();
		  if(msg != null) {
			  logger.info(msg);
			  producer.send(new ProducerRecord<String, String>("twitter",null,msg),new Callback() {
				
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if(exception!=null)
						logger.error("something bad happened");
					
				}
			});
		  }
		}
		logger.info("end of application");
	}
	
	
	
	
	private Client createTwitterClient(BlockingQueue<String> msgQueue) {
		Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		endpoint .trackTerms(Arrays.asList("trump"));
		
		Authentication auth = new OAuth1(CONSUMER_KEY,CONSUMER_SECRET,TOKEN,SECRET);
		
		
		return new ClientBuilder()
				.name("host-burg-client")
				.endpoint(endpoint)
				.hosts(hosts)
				.authentication(auth)
				.processor(new StringDelimitedProcessor(msgQueue))
				.build();
	}

	private KafkaProducer<String, String> createProducer() {
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		return producer;
	}


}
