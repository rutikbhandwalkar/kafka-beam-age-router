package com.example.agefilter;

import com.example.agefilter.service.BeamPipeline;
import com.example.agefilter.service.KafkaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.List;

@SpringBootApplication
public class AgeRouterApplication implements CommandLineRunner {

    @Autowired
    private KafkaService kafkaService;

    public static void main(String[] args) {
        SpringApplication.run(AgeRouterApplication.class, args);
    }

    @Override
    public void run(String... args) {


    }
}
