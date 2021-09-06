package com.mclabs.kafka.basics;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemo {
	public static void main(String[] args) {
		
		final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);
		
		String bootstrapServer = "localhost:9092";

		Properties prop = new Properties();
		prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		
		// Kafka usually send data by BYTES, our case, kafka wants to send as string
		prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create the producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

		// create a producer record
		String topic = "first_java_kafka_topic";
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,
				"Hello, this data has been send by jithin through kafka");


		// send data - async
		/** 1. Simple step */
//		producer.send(record);

		/** 2. Callback */
//		producer.send(record, new Callback() {
//			
//			public void onCompletion(RecordMetadata metadata, Exception exception) {
//				if (exception == null) {
//					logger.info("Received new metadata \n " + metadata.topic() + "\n" + metadata.partition() + "\n"
//							+ metadata.offset() + "\n" + metadata.timestamp());
//				} else {
//					logger.error("error while producing");
//				}
//				
//			}	
//		});

		/** 3. Send with key */
		demoWithSendKeys(producer, topic, logger);

		// flush data
		producer.flush();

		// flush and close producer
		producer.close();
	}

	public static void demoWithSendKeys(KafkaProducer producer, String topic, final Logger logger) {
		int limit = 10;
		for (int i = 0; i < 10; i++) {

			String value = "new hello world " + Integer.toString(i);
			String key = "id_" + Integer.toString(i);
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
			logger.info("key: " + key);

			producer.send(record, new Callback() {
				public void onCompletion(RecordMetadata recordMetadata, Exception e) {
					// executes every time a record is successfully sent or an exception is thrown
					if (e == null) {
						// the record was successfully sent
						logger.info("Received new metadata. \n" + "Topic:" + recordMetadata.topic() + "\n"
								+ "Partition: " + recordMetadata.partition() + "\n" + "Offset: "
								+ recordMetadata.offset() + "\n" + "Timestamp: " + recordMetadata.timestamp());
					} else {
						logger.error("Error while producing", e);
					}
				}
			});

		}
	}
}
