package com.example.agefilter.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.Objects;

public class Person implements Serializable {
    private String name;
    private String dob;
    private String address;

    public Person() {}

    public Person(String name, String dob) {
        this.name = name;
        this.dob = dob;
    }

    public Person(String name, String dob, String address) {
        this.name = name;
        this.dob = dob;
        this.address = address;
    }

    // Getters
    public String getName() {
        return name;
    }

    public String getDob() {
        return dob;
    }

    public String getAddress() {
        return address;
    }

    // Setters
    public void setName(String name) {
        this.name = name;
    }

    public void setDob(String dob) {
        this.dob = dob;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @JsonIgnore
    public LocalDate getDobAsLocalDate() {
        if (dob == null || dob.isEmpty()) {
            return null;
        }
        return LocalDate.parse(dob);
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", dob='" + dob + '\'' +
                ", address='" + address + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Person person = (Person) o;
        // Compare name, dob, and address fields for equality
        return Objects.equals(name, person.name) &&
                Objects.equals(dob, person.dob) &&
                Objects.equals(address, person.address);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dob, address); // Generate hash code from name, dob, and address
    }
}
