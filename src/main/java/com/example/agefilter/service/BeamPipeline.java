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

public class BeamPipeline {

    private static final Logger logger = LoggerFactory.getLogger(BeamPipeline.class);

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String SOURCE_TOPIC = "SOURCE_TOPIC";
    private static final String EVEN_TOPIC = "EVEN_TOPIC";
    private static final String ODD_TOPIC = "ODD_TOPIC";

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    public static void runPipeline() {
        FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
        options.setRunner(FlinkRunner.class);
        options.setStreaming(true);
        options.setParallelism(2);

        Pipeline pipeline = Pipeline.create(options);

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
                            Person person = MAPPER.readValue(json, Person.class);
                            logger.info("Received: {}", person);
                            return person;
                        } catch (JsonProcessingException e) {
                            logger.error("Failed to parse JSON: {}", json, e);
                            throw new RuntimeException(e);
                        }
                    }
                }));

        var personToJson = MapElements.via(new SimpleFunction<Person, String>() {
            @Override
            public String apply(Person person) {
                try {
                    String json = MAPPER.writeValueAsString(person);
                    logger.info("Sending: {}", json);
                    return json;
                } catch (Exception e) {
                    logger.error("Failed to serialize Person: {}", person, e);
                    throw new RuntimeException(e);
                }
            }
        });

        persons
                .apply("FilterEvenAge", Filter.by(person -> {
                    int age = calculateAge(person);
                    boolean isEven = age % 2 == 0;
                    logger.info("👤 {} is {} years old → {}", person.getName(), age, isEven ? "EVEN" : "ODD");
                    return isEven;
                }))
                .apply("SerializeEvenPersons", personToJson)
                .apply("WriteEvenToKafka",
                        KafkaIO.<Void, String>write()
                                .withBootstrapServers(BOOTSTRAP_SERVERS)
                                .withTopic(EVEN_TOPIC)
                                .withValueSerializer(StringSerializer.class)
                                .values());

        persons
                .apply("FilterOddAge", Filter.by(person -> {
                    int age = calculateAge(person);
                    boolean isOdd = age % 2 != 0;
                    logger.info("👤 {} is {} years old → {}", person.getName(), age, isOdd ? "ODD" : "EVEN");
                    return isOdd;
                }))
                .apply("SerializeOddPersons", personToJson)
                .apply("WriteOddToKafka",
                        KafkaIO.<Void, String>write()
                                .withBootstrapServers(BOOTSTRAP_SERVERS)
                                .withTopic(ODD_TOPIC)
                                .withValueSerializer(StringSerializer.class)
                                .values());

        pipeline.run();
    }

    private static int calculateAge(Person person) {
        return Period.between(person.getDobAsLocalDate(), LocalDate.now()).getYears();
    }
}