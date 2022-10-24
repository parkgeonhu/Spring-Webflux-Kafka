package com.example.springwebfluxkafka.service.impl;

import com.example.springwebfluxkafka.service.KafkaService;
import com.example.springwebfluxkafka.service.MessageService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;


@Service
@RequiredArgsConstructor
public class MessageServiceImpl implements MessageService {

    private final KafkaService kafkaService;
    private final ObjectMapper objectMapper;

    @Value("${kafka.topic}")
    private String topic;

    @Override
    public Mono<String> send(String key, Object value) {
        try {
            return kafkaService
                    .send(topic, key, objectMapper.writeValueAsString(value))
                    .flatMap(b -> {
                        if (b) {
                            return Mono.just("success send message");
                        } else {
                            return Mono.just("fail send message");
                        }
                    });
        } catch (Exception e) {
            return Mono.error(e);
        }

    }
}
