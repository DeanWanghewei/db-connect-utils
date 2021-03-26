package com.dean.db.connect.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

/**
 * @author superdeanwanghewei@163.com
 * @date 2021/3/27 00:42
 */
public class KafkaProducerTest {
    private KafkaProducer<String, String> producer;
    private static String bootstrapServers = "127.0.0.1:9092";
    private static String StringSerializer = "org.apache.kafka.common.serialization.StringSerializer";

    @Before
    public void before() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer);
        producer = new KafkaProducer<String, String>(properties);
    }

    @Test
    public void sendData() {
        for (int i = 6; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("topicTest", String.format("key-%d", i), String.format("record-%d", i));
            producer.send(record);
        }
        producer.flush();
    }


    @After
    public void close() {
        producer.close();
    }

}
