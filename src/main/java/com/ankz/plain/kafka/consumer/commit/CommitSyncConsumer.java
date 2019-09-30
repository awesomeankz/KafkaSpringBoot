package com.ankz.plain.kafka.consumer.commit;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

public class CommitSyncConsumer implements Runnable {

	private final KafkaConsumer<String, String> consumer;
	private final List<String> topics;
	private final int id;

	// construct
	public CommitSyncConsumer(int id, String groupId, List<String> topics) {
		this.id = id;
		this.topics = topics;
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", groupId);

		props.put("enable.auto.commit", "false"); // disabling the auto-commit

		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		this.consumer = new KafkaConsumer<>(props);
	}

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
					/*
					 * https://www.confluent.io/blog/tutorial-getting-started-with-the-new-apache-
					 * kafka-0-9-consumer-client/
					 * 
					 * Committing offset after each message is processed. Using the commitSync API
					 * with no arguments commits the offsets returned in the last call to poll.
					 * 
					 * 
					 * calling commitSync api after processing message gives "atleast-once"
					 * guarantee.
					 * 
					 * if we want "atmost-once" guarantee, we need to call commitSync() before
					 * processing.
					 */

					try {
						consumer.commitSync();
					} catch (CommitFailedException e) {
						// application specific failure handling
					}
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
