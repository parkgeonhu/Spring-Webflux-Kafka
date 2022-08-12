package com.example.springwebfluxkafka.service.impl;

import com.example.springwebfluxkafka.model.exception.KafkaException;
import com.example.springwebfluxkafka.service.KafkaService;
import com.example.springwebfluxkafka.service.MessageService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

@Service
@RequiredArgsConstructor
public class MessageServcieImpl implements MessageService {

    private final KafkaService kafkaService;
    private final ObjectMapper objectMapper;

    @Value("${kafka.topic}")
    private String topic;

    @Override
    public Mono<String> send(String key, Object value) {
        try {
            return kafkaService.send(topic, key, objectMapper.writeValueAsString(value))
                    .map(b -> {
                        if (b) {
                            return "success send message";
                        } else {
                            return "fail send message";
                        }
                    });
        } catch (JsonProcessingException e) {
            return Mono.error(KafkaException.SEND_ERROR);
        }
    }
}
