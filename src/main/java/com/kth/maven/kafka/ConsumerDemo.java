/**
 * Huihe.com Inc.
 * Copyright (c) 2022-2020 All Rights Reserved.
 */
package com.kth.maven.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author: tianhui.ke
 * @version $Id: ConsumerDemo.java, v0.1 2022/8/10  0:39 tianhui.ke
 */
public class ConsumerDemo {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.0.127:9092");
        /**
         * consumer分组id
         */
        properties.put("group.id", "zabbix_perf");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "100");
        /**
         * earliest
         *   当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
         *   latest
         *   当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
         *   none
         *   topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
         *
         */
        properties.put("auto.offset.reset", "earliest");
        properties.put("session.timeout.ms", "30000");
        /**
         * 反序列化
         * 把kafka集群二进制消息反序列化指定类型。
         */
        properties.put("key.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Arrays.asList("kth_topic_02"));
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);//100是超时时间
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, value = %s", record.offset(), record.value());
                System.out.println();
            }
        }

    }
}
