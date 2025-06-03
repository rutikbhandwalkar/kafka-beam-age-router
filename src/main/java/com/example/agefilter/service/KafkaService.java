package com.example.agefilter.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

// Service class to publish messages to Kafka topics
@Service
public class KafkaService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaService.class);

    // KafkaTemplate is a convenient wrapper for sending messages to Kafka.
    // It handles the underlying Producer API details.
    private final KafkaTemplate<String, String> kafkaTemplate;

    // Constructor injection of KafkaTemplate
    public KafkaService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    // Publishes the given message to the specified Kafka topic
    public void publish(String topic, String message) {
        logger.info("Publishing message to topic '{}': {}", topic, message);
        kafkaTemplate.send(topic, message); // Send message to Kafka topic
    }
}
