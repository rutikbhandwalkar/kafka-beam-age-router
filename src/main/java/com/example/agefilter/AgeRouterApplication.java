package com.example.agefilter;

import com.example.agefilter.service.BeamPipeline;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class AgeRouterApplication {

    public static void main(String[] args) {
        SpringApplication.run(AgeRouterApplication.class, args);
        BeamPipeline.runPipeline();
    }
}
