package com.ankz.sb.kafka.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class Consumer {
	
	
	private final Logger logger = LoggerFactory.getLogger(Consumer.class);
	private static final String TOPIC = "kafkaspringboot";
	private static final String GROUP_ID = "group_id";
	
	@KafkaListener(topics = TOPIC, groupId = GROUP_ID)
	public void consume(String message){
	logger.info(String.format("$$ -> Consumed Message -> %s",message));
	}

	
	
	
}
