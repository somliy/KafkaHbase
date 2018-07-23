package com.wfu.kafkahbasetest.bean;


import com.alibaba.fastjson.JSONObject;
import com.wfu.kafkahbasetest.thread.HandleData;
import com.wfu.kafkahbasetest.util.HBaseUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;

import java.io.IOException;
import java.util.UUID;
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
    public void listenPartition0(String message) throws IOException {
        LOGGER.info("received message 00='{}'", message);
        LOGGER.info("thread ID:" + Thread.currentThread().getId());
        save();
        latch0.countDown();
    }

    @KafkaListener(id = "id1", topicPartitions = {@TopicPartition(topic = "${spring.kafka.template.default-topic}", partitions = {"1"})})
    public void listenPartition1(String message) throws IOException {
        LOGGER.info("received message 01='{}'", message);
        LOGGER.info("thread ID:" + Thread.currentThread().getId());
        save();
        latch1.countDown();
    }

    @KafkaListener(id = "id2", topicPartitions = {@TopicPartition(topic = "${spring.kafka.template.default-topic}", partitions = {"2"})})
    public void listenPartition2(String message) throws IOException {
        LOGGER.info("received message 02='{}'", message);
        LOGGER.info("thread ID:" + Thread.currentThread().getId());
        save();
        latch2.countDown();
    }

    @KafkaListener(id = "id3", topicPartitions = {@TopicPartition(topic = "${spring.kafka.template.default-topic}", partitions = {"3"})})
    public void listenPartition3(String message) throws IOException {
        LOGGER.info("received message 03='{}'", message);
        LOGGER.info("thread ID:" + Thread.currentThread().getId());
        save();
        latch3.countDown();
    }

    @KafkaListener(id = "id4", topicPartitions = {@TopicPartition(topic = "${spring.kafka.template.default-topic}", partitions = {"4"})})
    public void listenPartition4(String message) throws IOException {
        LOGGER.info("received message 04='{}'", message);
        LOGGER.info("thread ID:" + Thread.currentThread().getId());
        save();
        latch4.countDown();
    }

    private void save( ) throws IOException {
//        LOGGER.info(message);
        if(HandleData.dohandleData()){
            LOGGER.info("写入成功...");
        }else{
            LOGGER.error("写入失败...");
        }
    }
}