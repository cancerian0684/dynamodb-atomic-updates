package com.shunya.tutorial.dynamodbatomiccounter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class AtomicCounterApp {

	public static void main(String[] args) {
		SpringApplication.run(AtomicCounterApp.class, args);
	}
}
