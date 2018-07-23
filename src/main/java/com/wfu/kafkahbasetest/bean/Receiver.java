package com.wfu.kafkahbasetest.bean;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;


public class Receiver {

//    @Autowired
//    private MessageHandle messageHandle;
    private static final Logger LOGGER = LoggerFactory.getLogger(Receiver.class);

    private CountDownLatch latch0 = new CountDownLatch(5);
    private CountDownLatch latch1 = new CountDownLatch(5);
    private CountDownLatch latch2 = new CountDownLatch(5);
    private CountDownLatch latch3 = new CountDownLatch(5);
    private CountDownLatch latch4 = new CountDownLatch(5);

    @KafkaListener(id = "id0", topicPartitions = {@TopicPartition(topic = "${spring.kafka.template.default-topic}", partitions = {"0"})})
    public void listenPartition0(String message) {
        LOGGER.info("received message 00='{}'", message);
        LOGGER.info("thread ID:" + Thread.currentThread().getId());
        latch0.countDown();
    }

    @KafkaListener(id = "id1", topicPartitions = {@TopicPartition(topic = "${spring.kafka.template.default-topic}", partitions = {"1"})})
    public void listenPartition1(String message) {
        LOGGER.info("received message 01='{}'", message);
        LOGGER.info("thread ID:" + Thread.currentThread().getId());
        latch1.countDown();
    }

    @KafkaListener(id = "id2", topicPartitions = {@TopicPartition(topic = "${spring.kafka.template.default-topic}", partitions = {"2"})})
    public void listenPartition2(String message) {
        LOGGER.info("received message 02='{}'", message);
        LOGGER.info("thread ID:" + Thread.currentThread().getId());
        latch2.countDown();
    }

    @KafkaListener(id = "id3", topicPartitions = {@TopicPartition(topic = "${spring.kafka.template.default-topic}", partitions = {"3"})})
    public void listenPartition3(String message) {
        LOGGER.info("received message 03='{}'", message);
        LOGGER.info("thread ID:" + Thread.currentThread().getId());
        latch3.countDown();
    }

    @KafkaListener(id = "id4", topicPartitions = {@TopicPartition(topic = "${spring.kafka.template.default-topic}", partitions = {"4"})})
    public void listenPartition4(String message) {
        LOGGER.info("received message 04='{}'", message);
        LOGGER.info("thread ID:" + Thread.currentThread().getId());
        latch4.countDown();
    }
}