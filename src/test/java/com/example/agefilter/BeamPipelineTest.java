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

    // Rule to enable use of the Beam testing pipeline
    @Rule
    public final TestPipeline pipeline = TestPipeline.create();

    // Fixed date used for consistent age calculation in tests
    private static final LocalDate CURRENT_TEST_DATE = LocalDate.of(2025, Month.JUNE, 3);

    @Test
    public void testPersonToJsonSerialization() {
        // Create a test person instance
        Person person = new Person("Ajay", "2000-01-01", "Pune");

        // Create a PCollection from the single Person object
        PCollection<Person> input = pipeline.apply("CreatePerson", Create.of(person));

        // Apply transformation to serialize the person to JSON
        PCollection<String> output = input.apply("SerializePersonToJson",
                MapElements.into(PersonUtils.STRING_TYPE_DESCRIPTOR)
                        .via(PersonUtils::serialize));

        // Assert that the output JSON matches the expected value
        PAssert.that(output).containsInAnyOrder("{\"name\":\"Ajay\",\"dob\":\"2000-01-01\",\"address\":\"Pune\"}");

        // Run the pipeline
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testFilterEvenAge() {
        // Create persons with even and odd ages
        Person even = new Person("Even", CURRENT_TEST_DATE.minusYears(24).toString());
        Person odd = new Person("Odd", CURRENT_TEST_DATE.minusYears(25).toString());

        // Create a PCollection from these two persons
        PCollection<Person> input = pipeline.apply("CreatePersonsForEvenFilter", Create.of(even, odd));

        // Filter only even-aged persons
        PCollection<Person> evenAged = input.apply("FilterEvenAgedPersons", Filter.by(person ->
                PersonUtils.calculateAge(person, CURRENT_TEST_DATE) % 2 == 0
        ));

        // Assert that only the even-aged person is present in the output
        PAssert.that(evenAged).containsInAnyOrder(even);

        // Run the pipeline
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testFilterOddAge() {
        // Create persons with even and odd ages
        Person even = new Person("Even", CURRENT_TEST_DATE.minusYears(24).toString());
        Person odd = new Person("Odd", CURRENT_TEST_DATE.minusYears(25).toString());

        // Create a PCollection from these two persons
        PCollection<Person> input = pipeline.apply("CreatePersonsForOddFilter", Create.of(even, odd));

        // Filter only odd-aged persons
        PCollection<Person> oddAged = input.apply("FilterOddAgedPersons", Filter.by(person ->
                PersonUtils.calculateAge(person, CURRENT_TEST_DATE) % 2 != 0
        ));

        // Assert that only the odd-aged person is present in the output
        PAssert.that(oddAged).containsInAnyOrder(odd);

        // Run the pipeline
        pipeline.run().waitUntilFinish();
    }
}
