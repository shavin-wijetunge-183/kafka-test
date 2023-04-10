package com.shavin.xcpem;

import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class XcpemApplication {

	public static void main(String[] args) {
		SpringApplication.run(XcpemApplication.class, args);
	}

}
