/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example;

import java.util.Scanner;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.SendTo;

/**
 * @author Gary Russell
 *
 */
@SpringBootApplication
@EnableBinding(Processor.class)
public class KafkaScstApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaScstApplication.class, args).close();
	}

	@Bean
	public ApplicationRunner runner() {
		return new Publisher("rjug.dest");
	}

	@StreamListener(Processor.INPUT)
	@SendTo(Processor.OUTPUT)
	public String process(String in) {
		System.out.println(in + " (in processor)");
		if ("fail".equals(in)) {
			throw new RuntimeException("failed");
		}
		return in.toUpperCase();
	}

	@KafkaListener(id = "rjug.dest.out", topics = "rjug.dest.out")
	public void listen(byte[] in) {
		System.out.println(new String(in));
	}

}
