package com.ankz.plain.kafka.consumer.commit;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * 
 * @author ankit.kamal
 * 
 *         All previous approach commit the last offset of the batches fetched
 *         by poll(). In case, we need to commit offset in the middle of the
 *         batch we can pass the Map<TopicPartition, OffsetAndMetadata> .
 *         
 *
 */
public class CommitSpecifiedOffsets implements Runnable {

	private final KafkaConsumer<String, String> consumer;
	private final List<String> topics;
	private final int id;

	private Map<TopicPartition, OffsetAndMetadata> currentOffsetsMap = new HashMap<>();
	int count = 0;

	// construct
	public CommitSpecifiedOffsets(int id, String groupId, List<String> topics) {
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

					try {
						/*
						 * After reading each record, we update the offsets map with the offset of the
						 * next message we expect to process. This is where we’ll start reading next
						 * time we start.
						 * 
						 * this takes Map<TopicPartition, OffsetAndMetadata>; and we do +1 to manually
						 * to set offset. Both sync and async api are valid here.
						 * 
						 * we can  decide to commit current offsets every 1,000 records. if (count % 1000 == 0)
						 */
						consumer.commitSync(
								Collections.singletonMap(new TopicPartition(record.topic(), record.partition()),
										new OffsetAndMetadata(record.offset() + 1)));

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
