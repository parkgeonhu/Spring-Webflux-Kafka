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
    private final ReceiverOptions<String, Object> receiverOptions;

    private Disposable disposable;

//    @PostConstruct
//    public void init() {    // Consumer를 열어놓음
//        disposable = KafkaReceiver.create(receiverOptions).receive()
//                .doOnNext(processReceivedData())
//                .doOnError(e -> {
//                    System.out.println("Kafka read error");
//                    init();     // 에러 발생 시, consumer가 종료되고 재시작할 방법이 없기 때문에 error시 재시작
//                })
//                .subscribe();
//    }

    @PreDestroy
    public void destroy() {
        if (disposable != null) {
            disposable.dispose();
        }
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
