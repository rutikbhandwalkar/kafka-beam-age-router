package com.example.agefilter.util;

import com.example.agefilter.model.Person;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.beam.sdk.values.TypeDescriptor; // Import TypeDescriptor

import java.time.LocalDate;
import java.time.Period;

public class PersonUtils {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    // TypeDescriptor for String, used by MapElements.into() for type safety
    public static final TypeDescriptor<String> STRING_TYPE_DESCRIPTOR = TypeDescriptor.of(String.class);

    public static String serialize(Person person) {
        try {
            return MAPPER.writeValueAsString(person);
        } catch (JsonProcessingException e) {
            System.err.println("Failed to serialize Person to JSON: " + person);
            throw new RuntimeException("Failed to serialize Person to JSON", e);
        }
    }

    public static Person deserialize(String json) {
        try {
            return MAPPER.readValue(json, Person.class);
        } catch (JsonProcessingException e) {
            System.err.println("Failed to deserialize JSON to Person: " + json);
            throw new RuntimeException("Failed to deserialize JSON to Person", e);
        }
    }

    public static int calculateAge(Person person) {
        // Delegate to the overloaded method with the actual current date
        return calculateAge(person, LocalDate.now());
    }

    public static int calculateAge(Person person, LocalDate currentDate) {
        if (person == null) {
            throw new IllegalArgumentException("Person cannot be null for age calculation.");
        }
        LocalDate dobAsLocalDate = person.getDobAsLocalDate();
        if (dobAsLocalDate == null || currentDate == null) {
            throw new IllegalArgumentException("Date of Birth or Current Date cannot be null for age calculation. Person DOB: " + person.getDob() + ", Current Date: " + currentDate);
        }

        return Period.between(dobAsLocalDate, currentDate).getYears();
    }
}
