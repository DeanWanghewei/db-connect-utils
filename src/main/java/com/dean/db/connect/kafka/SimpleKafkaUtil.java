package com.dean.db.connect.kafka;

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.protocol.types.Field;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * @author superdeanwanghewei@163.com
 * @date 2021/3/17 23:29
 */
public class SimpleKafkaUtil {
    private static final String bootstrap = "";


    public static Properties kafkaProperties;

    private KafkaProducer<String, String> producer;

    void init(Properties properties) {
        producer = new KafkaProducer<String, String>(kafkaProperties);
    }

    public Future<RecordMetadata> sendData2Kafka(String topic, String data) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, data);
        return producer.send(record);
    }

    public Properties getProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.server", this.bootstrap);

        return properties;
    }

    public static void test(int x) {
        boolean index = x % 2 != 0;
    }

    public static void test2(int x) {
        boolean index2 = (x & 1) == 1;
    }

    public static void main(String[] args) {
        test(6);
        test2(6);
    }


}
