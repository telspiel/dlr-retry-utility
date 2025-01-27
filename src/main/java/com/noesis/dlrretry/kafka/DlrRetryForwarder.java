package com.noesis.dlrretry.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

public class DlrRetryForwarder {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	public void send(String topic, String payload) {
		logger.info("Sending payload='" + payload + "' to topic='" + topic + "'");
		kafkaTemplate.send(topic, payload);
	}
}