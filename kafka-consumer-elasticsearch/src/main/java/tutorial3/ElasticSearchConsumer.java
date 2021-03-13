package tutorial3;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    public static class Config {
        String bonsaiHostname;
        String bonsaiUsername;
        String bonsaiPassword;

        public String getBonsaiHostname() {
            return bonsaiHostname;
        }

        public void setBonsaiHostname(String bonsaiHostname) {
            this.bonsaiHostname = bonsaiHostname;
        }

        public String getBonsaiUsername() {
            return bonsaiUsername;
        }

        public void setBonsaiUsername(String bonsaiUsername) {
            this.bonsaiUsername = bonsaiUsername;
        }

        public String getBonsaiPassword() {
            return bonsaiPassword;
        }

        public void setBonsaiPassword(String bonsaiPassword) {
            this.bonsaiPassword = bonsaiPassword;
        }
    }

    public static void main(String[] args) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        Config config = objectMapper.readValue(new File("consumer-config.json"), Config.class);

        RestHighLevelClient client = createClient(config);

        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

        // NOTE: Delivery Semantics
        //       At-most-once
        //       Batches reads by setting the offset ahead of the records
        //       it is about to read, so if the consumer goes down and then
        //       comes back up, it will read from the offset, potentially
        //       missing records.
        //
        //       At-least-once (default behaviour) -- this is what you should aim to use.
        //       In this case the offsets are set after the records are read
        //       from kafka. If the consumer goes down then it might read the
        //       same record again (duplicates). Ensure your processing is
        //       idempotent.
        //
        //       Exactly once -- only with kafka to kafka workflows using kafka streams.

        // poll for new data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            // Use bulkRequest to send bulk data to elasticSearch
            BulkRequest bulkRequest = new BulkRequest();

            int recordCount = records.count();
            logger.info("Received " + recordCount + " records");
            for (ConsumerRecord<String, String> record : records) {
                String jsonString = record.value();

                // We can make this idempotent by adding an id to the indexRequest
                // Generic ID:
                // String idempotentID = record.topic() + "_" + record.partition() + "_" + record.offset();
                //
                // Using twitter's feed specific id:
                try {
                    String idempotentID = extractIDFromTweet(record.value());
                    logger.info(idempotentID);

                    IndexRequest indexRequest = new IndexRequest("twitter", "tweets", idempotentID).source(jsonString, XContentType.JSON);

                    bulkRequest.add(indexRequest);
                } catch (NullPointerException e) {
                    logger.warn("skipping bad data " + record.value());
                }

                // This is only needed if we intend to send a single request to elasticSearch
                // per message read from the consumer.
//                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
//                String id = indexResponse.getId();
//                logger.info(id);

                // Consumer Poll  Behaviour
                // Fetch.min.bytes (Default 1KB)
                // - Control how much min data to receive
                // - If higher number (e.g. 100KB) Reduces request count and increases throughput
                //   - Increase latency
                //
                // Max.poll.records (default 500)
                // - increase if the messages are small and there is a lot of ram
                // - worth increasing if you're getting 500 records on every request to increase throughput
                //
                // Max.partitions.fetch.bytes (default 1MB)
                // - max data returned per partition; many partitions will requires more ram
                //
                // Fetch.max.bytes (default 50MB)
                // - max data returned from all partitions combined
                //
                // Avoid touching these unless you're consumer is maxing out throughput already

                // Consumer Commit Strategy
                // auto commit is enabled by default and great for synchronous processing of batches
                // enable.auto.commit = true by default
                // auto.commit.interval.ms=5000 by default and every time poll is called
                //
                // Could be useful to have more control over when the commits happen so that you commit
                // after the processing of a batch of data.

                // Consumer Offset Reset Behaviour
                // When the offset is lost (this can happen if the consumer is down for more than the
                // retention period (7 days by default).
                // The broker setting offset.retention.minutes controls how long the offset is retained
                // The data and offset retention configs in the broker are different, and should match.
                //
                // To retrieve new offsets in this scenario a config of:
                // auto.offset.reset = latest -- wil read from the end
                // auto.offset.reset = earliest -- will read from the beginning
                // auto.offset.reset = none -- If no offset found an exception is thrown (require manual intervention)

                // Replaying data for Consumers
                // - Shutdown all the consumers in the consumer group
                // - Use `Kafka-consumer-groups` command to set offset to what you want
                // - Restart consumers

                // Controlling Consumer Liveliness
                // - Consumers in a group talk to a Consumer coordinator
                // - A hear beat is sent to this coordinator to detect when a consumer is down
                // - A rebalance is performed when a consumer does go down
                // - To avoid issues, consumers should process data fast and poll often
                //
                // Heart beat thread: Session.timeout.ms (default is 10 secs)
                // - If no heart beat in this period then a rebalance occurs
                // - Set lower for faster consumer rebalances
                // Heartbeat.interval.ms (default to 3 secs):
                // - Guideline is to set this as 1/3 of session.timeout.ms
                //
                // max.poll.interval.ms (default is 5  minutes):
                // - max amount of time between two poll calls before the consumer is considered offline
                // - This is used to detect a data processing issue with the consumer, incase the processing
                //   takes a long time.

                // Kafka Connect:
                // - Prebuilt connect "libraries" which retrieve data from popular sources (DBs, Salesforce, Twitter,
                //   etc.) and write data to popular destinations (DBs, Salesforce, twitter etc).
            }
            if (recordCount > 0) {
                BulkResponse bulkItemResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                logger.info("Committing offsets...");
                consumer.commitSync();
                logger.info("Offsets have been committed");

                // Just slowing the processing down so that we can see what's going on
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

//        client.close();
    }

    private static String extractIDFromTweet(String tweetJSON) throws Exception {
        ObjectNode node = new ObjectMapper().readValue(tweetJSON, ObjectNode.class);
        if (node.has("id_str")) {
            return node.get("id_str").asText();
        }
        throw new Exception("cannot find id_str in tweet json");
    }

    public static RestHighLevelClient createClient(Config config) {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(config.bonsaiUsername, config.bonsaiPassword));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(config.bonsaiHostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {
        // Set Consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kafka-demo-elasticsearch");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable autocommit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10"); // Setting this low just to be able to follow the logs

        // Create a Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }
}
