/*
 * Copyright 2018 the original author or authors.
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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * @author Gary Russell
 * @since 5.1
 *
 */
public class Publisher implements ApplicationRunner {

	private final String topic;

	@Autowired
	private KafkaTemplate<String, String> template;

	public Publisher(String topic) {
		this.topic = topic;
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {
		Scanner scanner = new Scanner(System.in);
		String line = scanner.nextLine();
		while (!line.equals("exit")) {
			this.template.send(this.topic, line);
			line = scanner.nextLine();
		}
		scanner.close();
	}

}
