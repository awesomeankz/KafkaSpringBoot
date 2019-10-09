package com.ankz.plain.kafka.consumer.rebalance;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

public class HandleRebalanceExample implements Runnable {

	private final KafkaConsumer<String, String> consumer;
	private final List<String> topics;
	private final int id;

	private Map<TopicPartition, OffsetAndMetadata> currentOffsetsMap = new HashMap<>();
	int count = 0;

	// construct
	public HandleRebalanceExample(int id, String groupId, List<String> topics) {
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

			consumer.subscribe(topics, new HandleRebalance()); // 4. pass the ConsumerRebalanceListener to the
																// subscribe() method so it will get invoked by the
																// consumer

			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
				for (ConsumerRecord<String, String> record : records) {
					Map<String, Object> data = new HashMap<>();
					data.put("partition", record.partition());
					data.put("offset", record.offset());
					data.put("value", record.value());
					System.out.println(this.id + ": " + data);

					currentOffsetsMap.put(new TopicPartition(record.topic(), record.partition()),
							new OffsetAndMetadata(record.offset() + 1, null));

					try {
						consumer.commitAsync(currentOffsetsMap, null);

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

	private class HandleRebalance implements ConsumerRebalanceListener { // 1. implementing a ConsumerRebalanceListener.

		@Override
		public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

			System.out.println("Lost partitions in rebalance. " + "Committing current offsets:" + currentOffsetsMap);
			consumer.commitSync(currentOffsetsMap);// 3. when we are about to lose a partition due to re-balancing, we
													// need to commit offsets. we are committing the latest offsets
													// we’ve processed, not the latest offsets in the batch we are still
													// processing

		}

		@Override
		public void onPartitionsAssigned(Collection<TopicPartition> partitions) {// 2. we don’t need to do anything when
																					// we get a new partition
			// TODO Auto-generated method stub

		}

	}// end of inner class

}
