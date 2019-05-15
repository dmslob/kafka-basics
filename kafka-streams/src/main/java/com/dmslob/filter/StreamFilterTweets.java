package com.dmslob.filter;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamFilterTweets {

    private static final String HOST = "127.0.0.1:9092";

    private static JsonParser jsonParser = new JsonParser();

    public static void main(String[] args) {
        // create props
        Properties props = new Properties();
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, HOST);
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // input topic
        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets");
        KStream<String, String> filterStream = inputTopic.filter((k, jsonTweet) -> extractUserFollowersInTweet(jsonTweet) > 10000);

        // But before
        // kafka-topics --zookeeper 127.0.0.1:2181 --create --topic important_tweets --partitions 3 --replication-factor 1
        filterStream.to("important_tweets");

        // build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);

        // start stream
        kafkaStreams.start();

        // kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic important_tweets --from-beginning
    }

    private static Integer extractUserFollowersInTweet(String jsonTweet) {
        try {
            return jsonParser.parse(jsonTweet)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        } catch (NullPointerException e) {
            return 0;
        }
    }
}
