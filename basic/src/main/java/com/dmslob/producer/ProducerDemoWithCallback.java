package com.dmslob.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    private static final String HOST = "127.0.0.1:9091";
    private static final String TOPIC = "first_topic";

    public static void main(String[] args) {
        // create Producer props
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        // create producer record
        for (int i = 0; i < 10; i++) {

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC, "message " + Integer.toString(i));
            // send data - asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is successfully sent or an exception is thrown
                    if (null == e) {
                        LOGGER.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        LOGGER.error("Error while producing", e);
                    }
                }
            });
        }

        producer.flush();
        producer.close();

        // After run application
        // kafka-topics --zookeeper 127.0.0.1:2181 --topic first_topic --describe
        // kafka-console-consumer --bootstrap-server 127.0.0.1:9091 --topic first_topic --from-beginning
    }
}
