package com.ankz.plain.kafka.consumer.rebalance;

import java.time.Duration;
import java.util.Collection;
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


public class ConsumeSpecifiedOffsets implements Runnable {
	


	private final KafkaConsumer<String, String> consumer;
	private final List<String> topics;
	private final int id;

	private Map<TopicPartition, OffsetAndMetadata> currentOffsetsMap = new HashMap<>();
	int count = 0;

	// construct
	public ConsumeSpecifiedOffsets(int id, String groupId, List<String> topics) {
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

			consumer.subscribe(topics, new SaveOffsetOnRebalance()); // 4. pass the ConsumerRebalanceListener to the
																// subscribe() method so it will get invoked by the
																// consumer

			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(0);
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

	private class SaveOffsetOnRebalance implements ConsumerRebalanceListener { // 1. implementing a ConsumerRebalanceListener.

		@Override
		public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			
			commitDBTransaction(); //this will commit offsets to DB when re-balance is initiated. 
		}

		private void commitDBTransaction() {
			// TODO This will commit the recenly processed message offset to DB.
			
		}

		@Override
		public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
			
			for(TopicPartition partition: partitions) {
				consumer.seek(partition, getOffsetFromDB(partition)); //to seek offset from DB when re-balance is over
				}
		}

		private long getOffsetFromDB(TopicPartition partition) {
			// TODO This will seek the last stored offset from DB.
			return 0;
		}

	}// end of inner class



}
