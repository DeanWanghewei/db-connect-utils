package com.dean.db.connect.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author superdeanwanghewei@163.com
 * @date 2021/3/25 00:21
 */

public class KafkaConsumerTest {

    private KafkaConsumer<String, String> consumer;
    private static String bootstrapServer = "127.0.0.1:9092";
    private static String keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
    private static String valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";

    @Before
    public void before() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "application-group-" + System.currentTimeMillis());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton("enriched_pageviews"));
    }

    /**
     * 获取所有topic信息
     */
    @Test
    public void listTopics() {
        Map<String, List<PartitionInfo>> topics = consumer.listTopics(Duration.ofMinutes(1));
        System.out.println(topics.keySet());
    }

    /**
     * 消费数据
     */
    @Test
    public void pullRecord() {
        while (true) {
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(5000));
            if (poll == null || poll.isEmpty()) {
                System.out.println("消费完成");
                break;
            }
            for (ConsumerRecord<String, String> record : poll) {
                System.out.println(
                        String.format("topic[%s],key[%s],value[%s],offset[%d],timestamp[%d]",
                                record.topic(), record.key(), record.value(), record.offset(), record.timestamp()));

            }

        }

    }

    @After
    public void close() {
        consumer.close();
    }
}
