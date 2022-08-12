package com.example.springwebfluxkafka.controller;

import com.example.springwebfluxkafka.model.Message;
import com.example.springwebfluxkafka.service.MessageService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/message")
@RequiredArgsConstructor
public class MessageController {

    private final MessageService messageService;

    @PostMapping
    public Mono<String> produceMessage(@RequestBody Mono<Message> message) {
        return message
                .flatMap(msg -> messageService.send(msg.getName(), msg));
    }
}
