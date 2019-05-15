package com.dmslob.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    private static final String HOST = "127.0.0.1:9091";
    private static final String TOPIC = "first_topic";

    public static void main(String[] args) {
        // create Producer props kafka-basics
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        // create producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC, "First topic");
        // send data - asynchronous
        producer.send(record);
        producer.flush();
        producer.close();
    }
}
