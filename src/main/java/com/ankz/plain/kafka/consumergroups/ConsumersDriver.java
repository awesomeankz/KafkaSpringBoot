package com.ankz.plain.kafka.consumergroups;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 
 * @author ankit.kamal
 * 
 *         This is driver program where we can set numOfThread = numOfConsumers
 *         running withing the given group-id.
 * 
 *         This example submits the three runnable consumers to an executor.
 *         Each thread is given a separate id so that you can see which thread
 *         is receiving data. The shutdown hook will be invoked when you stop
 *         the process, which will halt the three threads using wakeup and wait
 *         for them to shutdown.
 * 
 * 
 */
public class ConsumersDriver {

	public static void main(String[] args) {

		int numConsumers = 3;
		String groupId = "consumer-tutorial-group";
		List<String> topics = Arrays.asList("consumer-tutorial");
		ExecutorService executor = Executors.newFixedThreadPool(numConsumers);

		final List<ConsumerLoop> consumers = new ArrayList<>();
		for (int i = 0; i < numConsumers; i++) {
			ConsumerLoop consumer = new ConsumerLoop(i, groupId, topics); // here each conumer instance is executed in its own thread-id.
			consumers.add(consumer);
			executor.submit(consumer);
		}

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				for (ConsumerLoop consumer : consumers) {
					consumer.shutdown();
				}
				executor.shutdown();
				try {
					executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					e.printStackTrace();
					;
				}
			}
		});

		/*
		 * This example submits the three runnable consumers to an executor. Each thread
		 * is given a separate id so that you can see which thread is receiving data.
		 */

		/*
		 * Your code is reusing the same consumer instance for each thread in the pool.
		 * This is a bad idea, because calling the same method on the same instance of
		 * something is generally not considered threadsafe. Theoretically it should
		 * work just fine, but there is a problem that remains: You're calling shutdown
		 * on the same Consumer instance for each thread. That's theoretically not a
		 * major problem, because the method is written in a way that allows this.
		 */

	}
}
