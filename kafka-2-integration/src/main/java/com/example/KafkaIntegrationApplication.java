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

package com.example;

import java.util.Scanner;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.config.ContainerProperties;

/**
 * @author Gary Russell
 *
 */
@SpringBootApplication
public class KafkaIntegrationApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaIntegrationApplication.class, args).close();
	}

	@Bean
	public ApplicationRunner runner(KafkaTemplate<String, String> template) {
		return args -> {
			String line = "";
			Scanner scanner = new Scanner(System.in);
			while (!line.equals("exit")) {
				line = scanner.nextLine();
				template.send("rjugInt", line);
			}
			scanner.close();
		};
	}

	@Bean
	public IntegrationFlow flow(ConsumerFactory<String, String> consumerFactory,
			KafkaTemplate<Object, Object> template) {
		ContainerProperties containerProperties = new ContainerProperties("rjugInt");
		containerProperties.setGroupId("rjugInt");
		return IntegrationFlows.from(Kafka.messageDrivenChannelAdapter(consumerFactory, containerProperties))
				.filter(p -> !p.equals("ignore"))
				.<String, String>transform(String::toUpperCase)
				.<String, String>transform(s -> s + s)
				.handle(Kafka.outboundChannelAdapter(template).topic("rjugIntOut"))
				.get();
	}

	@KafkaListener(topics = "rjugIntOut", groupId = "rjugInt")
	public void listen(String in) {
		System.out.println(in);
	}

}
