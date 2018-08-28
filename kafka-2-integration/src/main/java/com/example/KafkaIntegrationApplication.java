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

import org.apache.kafka.clients.admin.NewTopic;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.handler.LoggingHandler.Level;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.DefaultKafkaHeaderMapper;
import org.springframework.messaging.Message;

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
	public ApplicationRunner runner() {
		return new Publisher("rjugInt");
	}

	@Bean
	public IntegrationFlow flow(ConcurrentKafkaListenerContainerFactory<String, String> containerFactory,
			KafkaTemplate<String, String> template, ErrorHandler errorHandler) {

		ConcurrentMessageListenerContainer<String, String> container =
				containerFactory.createContainer("rjugInt");
		container.getContainerProperties().setGroupId("rjugInt");
		container.setErrorHandler(errorHandler);
		return IntegrationFlows.from(
					Kafka.messageDrivenChannelAdapter(container))
				.filter(p -> !p.equals("ignore"), e -> e.discardFlow(
						f -> f
							.log(Level.WARN, m -> "Discarding: " + m.getPayload())
							.channel("nullChannel")))
				.handle((p, h) -> {
					if ("fail".equals(p)) {
						throw new RuntimeException("failed");
					}
					return p;
				})
				.enrich(h -> h
						.headerExpression("originalPayload", "payload")
						.header("foo", "bar"))
				.<String, String>transform(String::toUpperCase)
				.<String, String>transform(s -> s + s)
				.handle(Kafka.outboundChannelAdapter(template)
						.topic("rjugIntOut")
						.headerMapper(new DefaultKafkaHeaderMapper()))
				.get();
	}

	@KafkaListener(topics = "rjugIntOut", groupId = "rjugInt")
	public void listen(Message<String> in) {
		System.out.println(in);
	}

	@Bean
	public ErrorHandler errorHandler(DeadLetterPublishingRecoverer recoverer) {
		return new SeekToCurrentErrorHandler((cr, e) -> {
			System.out.println(cr.value() + " failed after retries");
			recoverer.accept(cr, e);
		}, 3);
	}

	@Bean
	public DeadLetterPublishingRecoverer recoverer(KafkaTemplate<Object, Object> template) {
		return new DeadLetterPublishingRecoverer(template);
	}

	@KafkaListener(topics = "rjugInt.DLT", groupId = "rjugInt")
	public void listenDLT(String in) {
		System.out.println(in + " from dead-letter topic");
	}

	@Bean
	public NewTopic rjugInt() {
		return new NewTopic("rjugInt", 10, (short) 1);
	}

	@Bean
	public NewTopic rjugIntDLT() {
		return new NewTopic("rjugInt.DLT", 10, (short) 1);
	}

	@Bean
	public NewTopic rjugIntOut() {
		return new NewTopic("rjugIntOut", 10, (short) 1);
	}

}
