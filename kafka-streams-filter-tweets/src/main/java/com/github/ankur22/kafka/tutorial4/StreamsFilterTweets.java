package com.github.ankur22.kafka.tutorial4;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets {
    public static void main(String[] args) {
        // Create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // Create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // input topic
        KStream<String,String> inputTopic = streamsBuilder.stream("twitter_tweets");
        KStream<String, String> filteredStream = inputTopic.filter(
                // Filter for tweets which have 10000 followers
                (k, jsonTweet) -> {
                    try {
                        return extractUserFollowersInTweets(jsonTweet) > 10000;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return false;
                }
        );
        filteredStream.to("important_tweets");

        // build topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        // Start streams app
        kafkaStreams.start();
    }

    private static int extractUserFollowersInTweets(String tweetJSON) throws Exception {
        ObjectNode node = new ObjectMapper().readValue(tweetJSON, ObjectNode.class);
        if (node.has("user")) {
            JsonNode user = node.get("user");
            if (user.has("followers_count")) {
                return user.get("followers_count").asInt();
            }
        }
        return 0;
    }
}
