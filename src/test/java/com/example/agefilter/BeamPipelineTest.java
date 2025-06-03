package com.example.agefilter;

import com.example.agefilter.model.Person;
import com.example.agefilter.util.PersonUtils;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;

import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import java.time.LocalDate;
import java.time.Month;

public class BeamPipelineTest {

    @Rule
    public final TestPipeline pipeline = TestPipeline.create();

    private static final LocalDate CURRENT_TEST_DATE = LocalDate.of(2025, Month.JUNE, 3); // Example fixed date

    @Test
    public void testPersonToJsonSerialization() {
        // Create a sample Person object with a specific date string
        Person person = new Person("Ajay", "2000-01-01", "Pune");

        // Create a PCollection from the single Person object
        PCollection<Person> input = pipeline.apply("CreatePerson", Create.of(person));

        // Apply a MapElements transform to serialize the Person to JSON using PersonUtils.
        // Changed from anonymous SimpleFunction to a method reference to avoid NotSerializableException.
        // PersonUtils.serialize is a static method, so it doesn't capture 'this' of BeamPipelineTest.
        PCollection<String> output = input.apply("SerializePersonToJson", MapElements.into(PersonUtils.STRING_TYPE_DESCRIPTOR)
                .via(PersonUtils::serialize)); // Use method reference for static serialization method

        // Assert that the output PCollection contains the expected JSON string
        PAssert.that(output).containsInAnyOrder("{\"name\":\"Ajay\",\"dob\":\"2000-01-01\",\"address\":\"Pune\"}");

        // Run the pipeline to execute the test
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testFilterEvenAge() {

        Person even = new Person("Even", CURRENT_TEST_DATE.minusYears(24).toString());
        Person odd = new Person("Odd", CURRENT_TEST_DATE.minusYears(25).toString());

        // Create a PCollection from these two persons
        PCollection<Person> input = pipeline.apply("CreatePersonsForEvenFilter", Create.of(even, odd));

        PCollection<Person> evenAged = input.apply("FilterEvenAgedPersons", Filter.by(person ->
                PersonUtils.calculateAge(person, CURRENT_TEST_DATE) % 2 == 0
        ));

        // Assert that the filtered PCollection contains only the even-aged person
        PAssert.that(evenAged).containsInAnyOrder(even);

        // Run the pipeline to execute the test
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testFilterOddAge() {
        Person even = new Person("Even", CURRENT_TEST_DATE.minusYears(24).toString());
        Person odd = new Person("Odd", CURRENT_TEST_DATE.minusYears(25).toString());

        // Create a PCollection from these two persons
        PCollection<Person> input = pipeline.apply("CreatePersonsForOddFilter", Create.of(even, odd));

        // Apply a Filter transform to keep only odd-aged persons.
        // Use the overloaded calculateAge method with CURRENT_TEST_DATE for deterministic results.
        PCollection<Person> oddAged = input.apply("FilterOddAgedPersons", Filter.by(person ->
                PersonUtils.calculateAge(person, CURRENT_TEST_DATE) % 2 != 0
        ));

        // Assert that the filtered PCollection contains only the odd-aged person
        PAssert.that(oddAged).containsInAnyOrder(odd);

        // Run the pipeline to execute the test
        pipeline.run().waitUntilFinish();
    }
}
