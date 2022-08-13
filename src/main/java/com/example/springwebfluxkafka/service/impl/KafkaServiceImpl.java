package com.example.springwebfluxkafka.service.impl;

import com.example.springwebfluxkafka.service.KafkaService;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.function.Consumer;

@Service
@RequiredArgsConstructor
public class KafkaServiceImpl implements KafkaService {
    private final KafkaSender<String, Object> kafkaSender;
    @PreDestroy
    public void destroy() {
        kafkaSender.close();
    }

    @Override
    public Mono<Boolean> send(String topic, String key, Object value) {
        return kafkaSender.createOutbound()
                .send(Mono.just(new ProducerRecord<>(topic, key, value)))  // 해당 topic으로 message 전송
                .then()
                .map(ret -> true)
                .onErrorResume(e -> {
                    System.out.println("Kafka send error");
                    return Mono.just(false);
                });
    }
}
