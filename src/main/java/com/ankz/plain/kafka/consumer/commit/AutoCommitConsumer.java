package com.ankz.plain.kafka.consumer.commit;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * 
 * @author ankit.kamal
 *
 *This is basic auto-commit as per config defined.
 *
 *<pre> second {@link CommitSyncConsumer}
 */
public class AutoCommitConsumer implements Runnable {

	private final KafkaConsumer<String, String> consumer;
	private final List<String> topics;
	private final int id;

	// construct
	public AutoCommitConsumer(int id, String groupId, List<String> topics) {
		this.id = id;
		this.topics = topics;
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", groupId);

		/*
		 * Note that using the automatic commits gives you “at least once” processing
		 * since the consumer guarantees that offsets are only committed for messages
		 * which have been returned to the application.
		 */
		props.put("enable.auto.commit", "true");// by default = true
		props.put("auto.commit.interval.ms", "1000"); // by default = 5000ms

		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		this.consumer = new KafkaConsumer<>(props);
	}

	// this is where poll-loop logic is implemented.
	@Override
	public void run() {

		try {

			consumer.subscribe(topics);

			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
				for (ConsumerRecord<String, String> record : records) {
					Map<String, Object> data = new HashMap<>();
					data.put("partition", record.partition());
					data.put("offset", record.offset());
					data.put("value", record.value());
					System.out.println(this.id + ": " + data);
				}
			}

		} catch (WakeupException e) {
			// ignore for shutdown
		} finally {
			consumer.close();
		}

	}

	public void shutdown() {
		consumer.wakeup();
	}

}
