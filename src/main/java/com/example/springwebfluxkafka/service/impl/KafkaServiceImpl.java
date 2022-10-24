package com.example.springwebfluxkafka.service.impl;

import com.example.springwebfluxkafka.service.KafkaService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import javax.annotation.PreDestroy;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaServiceImpl implements KafkaService {
    private final KafkaSender<String, Object> kafkaSender;
    @PreDestroy
    public void destroy() {
        kafkaSender.close();
    }

    @Override
    public Mono<Boolean> send(String topic, String key, Object value) {
        return kafkaSender.send(Mono.just(SenderRecord.create(new ProducerRecord<>(topic, key, value), null)))
                .collectList()
                .flatMap(list->{
                    SenderResult senderResult = list.get(0);
                    return Mono.just(senderResult.recordMetadata());
                })
                .doOnSuccess(System.out::println)
                .flatMap(ret-> Mono.just(true));
    }
}
