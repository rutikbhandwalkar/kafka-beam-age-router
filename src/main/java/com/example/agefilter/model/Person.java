package com.example.agefilter.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.Objects;

// Model class representing a person with name, date of birth, and address
public class Person implements Serializable {
    private String name;
    private String dob; // Date of birth in String format (e.g., "1990-01-01")
    private String address;

    // Default constructor
    public Person() {}

    // Constructor with name and dob
    public Person(String name, String dob) {
        this.name = name;
        this.dob = dob;
    }

    // Constructor with name, dob, and address
    public Person(String name, String dob, String address) {
        this.name = name;
        this.dob = dob;
        this.address = address;
    }

    // Getter for name
    public String getName() {
        return name;
    }

    // Getter for dob
    public String getDob() {
        return dob;
    }

    // Getter for address
    public String getAddress() {
        return address;
    }

    // Setter for name
    public void setName(String name) {
        this.name = name;
    }

    // Setter for dob
    public void setDob(String dob) {
        this.dob = dob;
    }

    // Setter for address
    public void setAddress(String address) {
        this.address = address;
    }

    // Converts dob String to LocalDate; ignored during JSON serialization
    @JsonIgnore
    public LocalDate getDobAsLocalDate() {
        if (dob == null || dob.isEmpty()) {
            return null;
        }
        return LocalDate.parse(dob);
    }

    // String representation of the Person object
    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", dob='" + dob + '\'' +
                ", address='" + address + '\'' +
                '}';
    }

    // Checks equality based on name, dob, and address
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Person person = (Person) o;
        return Objects.equals(name, person.name) &&
                Objects.equals(dob, person.dob) &&
                Objects.equals(address, person.address);
    }

    // Generates hash code based on name, dob, and address
    @Override
    public int hashCode() {
        return Objects.hash(name, dob, address);
    }
}
