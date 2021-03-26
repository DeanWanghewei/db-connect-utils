package com.dean.db.connect.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Before;

import java.util.Properties;

/**
 * @author superdeanwanghewei@163.com
 * @date 2021/3/25 00:21
 */

public class KafkaConsumerTest {

    private KafkaConsumer<String, String> consumer;
    private static String bootstrapServer = "127.0.0.1:9002";

    @Before
    public void before() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServer);
        consumer = new KafkaConsumer<String, String>(properties);
    }

    public void listTopics() {

    }
}
