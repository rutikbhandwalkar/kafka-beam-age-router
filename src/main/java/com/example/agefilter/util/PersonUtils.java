package com.example.agefilter.util;

import com.example.agefilter.model.Person;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.Period;

// Utility class for operations related to the Person model
public class PersonUtils {

    // Logger for logging serialization/deserialization errors
    private static final Logger logger = LoggerFactory.getLogger(PersonUtils.class);

    // ObjectMapper configured to handle Java 8 date/time types
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    // TypeDescriptor for String, used in Beam transformations
    public static final TypeDescriptor<String> STRING_TYPE_DESCRIPTOR = TypeDescriptor.of(String.class);

    // Serializes a Person object into its JSON string representation
    public static String serialize(Person person) {
        try {
            return MAPPER.writeValueAsString(person);
        } catch (JsonProcessingException e) {
            logger.error("Failed to serialize Person to JSON: {}", person, e);
            throw new RuntimeException("Failed to serialize Person to JSON", e);
        }
    }

    // Deserializes a JSON string into a Person object
    public static Person deserialize(String json) {
        try {
            return MAPPER.readValue(json, Person.class);
        } catch (JsonProcessingException e) {
            logger.error("Failed to deserialize JSON to Person: {}", json, e);
            throw new RuntimeException("Failed to deserialize JSON to Person", e);
        }
    }

    // Calculates the age of a Person using the current date
    public static int calculateAge(Person person) {
        return calculateAge(person, LocalDate.now());
    }

    // Calculates the age of a Person given a specific current date
    public static int calculateAge(Person person, LocalDate currentDate) {
        if (person == null) {
            throw new IllegalArgumentException("Person cannot be null for age calculation.");
        }

        // Convert dob string to LocalDate
        LocalDate dobAsLocalDate = person.getDobAsLocalDate();
        if (dobAsLocalDate == null || currentDate == null) {
            throw new IllegalArgumentException("Date of Birth or Current Date cannot be null for age calculation. Person DOB: " + person.getDob() + ", Current Date: " + currentDate);
        }

        // Calculate age in years between DOB and current date
        return Period.between(dobAsLocalDate, currentDate).getYears();
    }
}
