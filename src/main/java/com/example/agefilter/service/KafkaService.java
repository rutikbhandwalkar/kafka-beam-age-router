package com.example.agefilter.service;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaService {
    // KafkaTemplate is a convenient wrapper for sending messages to Kafka.
    // It handles the underlying Producer API details.
    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(String topic, String message) {
        System.out.println("Publishing message to topic '" + topic + "': " + message);
        kafkaTemplate.send(topic, message);
    }
}
