/*
 * Copyright 2017 the original author or authors.
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

package com.example.v1;

import java.util.Scanner;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;

/**
 * @author Gary Russell
 *
 */
@SpringBootApplication
public class KafkaSimpleApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaSimpleApplication.class, args).close();
	}

	@Bean
	public ApplicationRunner runner(KafkaTemplate<String, String> template) {
		return args -> {
			Scanner scanner = new Scanner(System.in);
			String line = scanner.nextLine();
			while (!line.equals("exit")) {
				template.send("rjug", line);
				line = scanner.nextLine();
			}
			scanner.close();
		};
	}

	@KafkaListener(topics = "rjug", groupId = "rjug")
	public void listen(String in,
			Consumer<?, ?> consumer,
			@Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
		System.out.println(in + " received from partition " + partition);
//		System.out.println(consumer.metrics());
	}

	@Bean
	public NewTopic rjug() {
		return new NewTopic("rjug", 1, (short) 1);
	}

}
