package com.ankz.plain.kafka;


import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * 
 * @author ankit.kamal
 *http://cloudurable.com/blog/kafka-tutorial-kafka-producer/index.html
 *
 *
 *http://kafka.apache.org/documentation.html#producerapi
 *http://kafka.apache.org/21/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html
 */
public class KafkaProducerExample {
	


	private final static String TOPIC = "thetechcheck";
	private final static String BOOTSTRAP_SERVERS = "192.168.56.101:9092";

	public static void main(String... args) throws Exception {
		if (args.length == 0) {
			runProducer(5);
		} else {
			runProducer(Integer.parseInt(args[0]));
		}
	}


	static void runProducer(final int sendMessageCount) throws Exception {
		final Producer<Long, String> producer = createProducer();
		long time = System.currentTimeMillis();

		try {
			for (long index = time; index < time + sendMessageCount; index++) {
				final ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, index, "Hey man " + index);

				RecordMetadata metadata = producer.send(record).get();

				long elapsedTime = System.currentTimeMillis() - time;
				System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n",
						record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);
				

			}
		} finally {
			producer.flush();
			producer.close();
		}
	}

	
	private static Producer<Long, String> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,  org.apache.kafka.common.serialization.LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,  org.apache.kafka.common.serialization.StringSerializer.class.getName());
		props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, org.apache.kafka.clients.producer.internals.DefaultPartitioner.class);
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		return new KafkaProducer<>(props);
	}

}
