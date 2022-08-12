package com.example.springwebfluxkafka.service;

import reactor.core.publisher.Mono;

public interface MessageService {
    Mono<String> send(String key, Object value);
}
