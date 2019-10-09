package com.ankz.plain.kafka.consumer.commit;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * 
 * @author ankit.kamal
 * 
 *         One drawback of manual commit is that the application is blocked
 *         until the broker responds to the commit request. This will limit the
 *         throughput of the application.
 * 
 *         Another option is this asynchronous commit API.
 *         
 *          <pre> Fourth {@link CommitSpecifiedOffsets}
 * 
 */
public class CommitAsyncConsumer implements Runnable {

	private final KafkaConsumer<String, String> consumer;
	private final List<String> topics;
	private final int id;

	// construct
	public CommitAsyncConsumer(int id, String groupId, List<String> topics) {
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
//						consumer.commitAsync(); // commitAsync API with no arguments commits the offsets returned in the
												// last call to poll and doesn't wait for the response from broker. but
												// there are no retries.

						
						//or we can have a callback handle to log failed commits with Async API.
						consumer.commitAsync(new OffsetCommitCallback() {
							public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets,
									Exception e) {
								if (e != null)
									System.out.printf("Commit failed for offsets {}", offsets, e);
							}
						});

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
