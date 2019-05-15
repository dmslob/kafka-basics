package com.dmslob.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
    private static final String HOST = "127.0.0.1:9091";
    private static final String TOPIC = "first_topic";

    // kafka-consumer-groups --bootstrap-server localhost:9091 --list
    //
    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();

        // ConsumerCoordinator class makes rebalancing all consumers
    }

    public void run() {
        String groupId = "test_group";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //earliest/latest/none

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(TOPIC));

        // poll for new data
        CountDownLatch latch = new CountDownLatch(1);

        LOGGER.info("Creating the consumer thread");
        Runnable runnable = new ConsumerThread(latch, consumer);
        new Thread(runnable).start();

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Caught shutdown hook");
            ((ConsumerThread) runnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            LOGGER.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            LOGGER.error("Application got interrupted", e);
        } finally {
            LOGGER.info("Application is closing");
        }
    }

    public class ConsumerThread implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerThread(CountDownLatch latch, KafkaConsumer<String, String> consumer) {
            this.latch = latch;
            this.consumer = consumer;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        LOGGER.info("Key: " + record.key() + ", " + record.value());
                        LOGGER.info("Partition: " + record.partition() + ", Offsets: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                LOGGER.info("Received shutdown signal");
            } finally {
                consumer.close();
                // tell our main code we are done with the consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            // Is a special method to interrupt consumer.poll()
            // It will throw the WakeUpException
            consumer.wakeup();
        }
    }
}
