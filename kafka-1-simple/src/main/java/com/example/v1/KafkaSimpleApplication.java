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

package com.example.v1;

import org.apache.kafka.clients.admin.NewTopic;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;

import com.example.Publisher;

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
	public ApplicationRunner runner() {
		return new Publisher("rjug");
	}

	@KafkaListener(topics = "rjug", groupId = "rjug")
	public void listen(String in) {
		System.out.println(in + " received");
	}

	@Bean
	public NewTopic rjug() {
		return new NewTopic("rjug", 1, (short) 1);
	}

}
