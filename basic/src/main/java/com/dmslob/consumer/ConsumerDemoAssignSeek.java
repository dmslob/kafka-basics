package com.dmslob.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);
    private static final String HOST = "127.0.0.1:9091";
    private static final String TOPIC = "first_topic";

    // kafka-consumer-groups --bootstrap-server localhost:9091 --list
    //
    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //earliest/latest/none

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        // assign and seek are mostly used to replay data or fetch a specific message
        // assign
        TopicPartition partitionReadFrom = new TopicPartition(TOPIC, 0);
        consumer.assign(Arrays.asList(partitionReadFrom));

        // seeek
        long offsetToReadFrom = 15L;
        consumer.seek(partitionReadFrom, offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;

        // poll for new data
        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesReadSoFar += 1;
                LOGGER.info("Key: " + record.key() + ", " + record.value());
                LOGGER.info("Partiotion: " + record.partition() + ", Offsets: " + record.offset());
                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
                    keepOnReading = false;
                    break;
                }
            }
        }

        LOGGER.info("Exiting the application");

        // ConsumerCoordinator class makes rebalancing all consumers
    }
}
