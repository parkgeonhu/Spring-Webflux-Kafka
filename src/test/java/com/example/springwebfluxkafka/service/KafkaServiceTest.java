package com.example.springwebfluxkafka.service;

import com.example.springwebfluxkafka.AbstractKafkaTest;
import com.example.springwebfluxkafka.service.impl.KafkaServiceImpl;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.internals.ConsumerFactory;
import reactor.kafka.sender.KafkaSender;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class KafkaServiceTest extends AbstractKafkaTest {
    private static final Logger log = LoggerFactory.getLogger(KafkaServiceTest.class.getName());

    KafkaService kafkaService;
    private Consumer<String, String> consumer;

    public KafkaSender kafkaSender(){
        return KafkaSender.create(senderOptions);
    }

    @Before
    public void setUp() throws Exception {
        KafkaSender<String, Object> kafkaSender = kafkaSender();
        kafkaService = new KafkaServiceImpl(kafkaSender);
        consumer = createConsumer();
    }

    @After
    public void tearDown() {
        if (consumer != null)
            consumer.close();
    }

    @Test
    public void test(){
        boolean result = kafkaService.send(topic, "test", "message").block();
        assertTrue(result);
        waitForMessages(consumer, 1, false);
    }

    private Consumer<String, String> createConsumer() throws Exception {
        String groupId = testName.getMethodName();
        Map<String, Object> consumerProps = consumerProps(groupId);
        Consumer<String, String> consumer = ConsumerFactory.INSTANCE.createConsumer(ReceiverOptions.<String, String>create(consumerProps));
        consumer.subscribe(Collections.singletonList(topic));
        consumer.poll(Duration.ofMillis(requestTimeoutMillis));
        return consumer;
    }


    private void waitForMessages(Consumer<String, String> consumer, int expectedCount, boolean checkMessageOrder) {
        int receivedCount = 0;
        long endTimeMillis = System.currentTimeMillis() + receiveTimeoutMillis;
        while (receivedCount < expectedCount && System.currentTimeMillis() < endTimeMillis) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            records.forEach(record -> assertEquals("test", record.key()));
            receivedCount += records.count();
        }
        if (checkMessageOrder)
            checkConsumedMessages();
        assertEquals(expectedCount, receivedCount);
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
        assertTrue("Unexpected message received: " + records.count(), records.isEmpty());
    }

}
