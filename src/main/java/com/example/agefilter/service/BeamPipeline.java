package com.example.agefilter.service;

import com.example.agefilter.model.Person;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.Period;

// Beam pipeline for reading from Kafka, filtering by age parity, and writing to separate topics
public class BeamPipeline {

    private static final Logger logger = LoggerFactory.getLogger(BeamPipeline.class);

    // Kafka configuration constants
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String SOURCE_TOPIC = "SOURCE_TOPIC";
    private static final String EVEN_TOPIC = "EVEN_TOPIC";
    private static final String ODD_TOPIC = "ODD_TOPIC";

    // Jackson ObjectMapper configured for Java 8 time support
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    // Main method to build and run the Beam pipeline
    public static void runPipeline() {
        // Set up Flink runner options
        FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
        options.setRunner(FlinkRunner.class);
        options.setStreaming(true);
        options.setParallelism(2);

        // Create the pipeline with options
        Pipeline pipeline = Pipeline.create(options);

        // Read from Kafka and convert JSON strings to Person objects
        var persons = pipeline
                .apply("ReadFromKafka",
                        KafkaIO.<String, String>read()
                                .withBootstrapServers(BOOTSTRAP_SERVERS)
                                .withTopic(SOURCE_TOPIC)
                                .withKeyDeserializer(StringDeserializer.class)
                                .withValueDeserializer(StringDeserializer.class)
                                .withoutMetadata())
                .apply("ExtractValues", MapElements.into(TypeDescriptor.of(String.class))
                        .via(kv -> kv.getValue()))
                .apply("JsonToPerson", MapElements.via(new SimpleFunction<String, Person>() {
                    @Override
                    public Person apply(String json) {
                        try {
                            Person person = MAPPER.readValue(json, Person.class); // Deserialize JSON to Person
                            logger.info("Received: {}", person);
                            return person;
                        } catch (JsonProcessingException e) {
                            logger.error("Failed to parse JSON: {}", json, e);
                            throw new RuntimeException(e);
                        }
                    }
                }));

        // Convert Person object to JSON string
        var personToJson = MapElements.via(new SimpleFunction<Person, String>() {
            @Override
            public String apply(Person person) {
                try {
                    String json = MAPPER.writeValueAsString(person); // Serialize Person to JSON
                    logger.info("Sending: {}", json);
                    return json;
                } catch (Exception e) {
                    logger.error("Failed to serialize Person: {}", person, e);
                    throw new RuntimeException(e);
                }
            }
        });

        // Filter persons with even age and write to EVEN_TOPIC
        persons
                .apply("FilterEvenAge", Filter.by(person -> {
                    int age = calculateAge(person); // Calculate age
                    boolean isEven = age % 2 == 0;
                    logger.info("👤 {} is {} years old → {}", person.getName(), age, isEven ? "EVEN" : "ODD");
                    return isEven;
                }))
                .apply("SerializeEvenPersons", personToJson) // Convert to JSON
                .apply("WriteEvenToKafka",
                        KafkaIO.<Void, String>write()
                                .withBootstrapServers(BOOTSTRAP_SERVERS)
                                .withTopic(EVEN_TOPIC)
                                .withValueSerializer(StringSerializer.class)
                                .values());

        // Filter persons with odd age and write to ODD_TOPIC
        persons
                .apply("FilterOddAge", Filter.by(person -> {
                    int age = calculateAge(person); // Calculate age
                    boolean isOdd = age % 2 != 0;
                    logger.info("👤 {} is {} years old → {}", person.getName(), age, isOdd ? "ODD" : "EVEN");
                    return isOdd;
                }))
                .apply("SerializeOddPersons", personToJson) // Convert to JSON
                .apply("WriteOddToKafka",
                        KafkaIO.<Void, String>write()
                                .withBootstrapServers(BOOTSTRAP_SERVERS)
                                .withTopic(ODD_TOPIC)
                                .withValueSerializer(StringSerializer.class)
                                .values());

        // Execute the pipeline
        pipeline.run();
    }

    // Helper method to calculate age from Person's DOB
    private static int calculateAge(Person person) {
        return Period.between(person.getDobAsLocalDate(), LocalDate.now()).getYears();
    }
}
