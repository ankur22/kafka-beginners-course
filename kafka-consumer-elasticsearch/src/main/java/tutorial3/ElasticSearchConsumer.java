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

                // Kafka partitions are made up of segments:
                // Segments are files with start and end offsets:
                //
                // |-partition 0---------------------------------------------|
                // | seg0 0-100 > seg1 101-200 > seg2 201-300 > seg3 301-350 |
                //
                // The last segment is the active segment (the one that is being written to), and there can only be one per partition.
                // log.segment.bytes: max size of a single segment in bytes (default: 1GB)
                // - smaller value:
                //  - more segs per partition
                //  - log compaction happen more often
                //  - kafka has to keep more files open
                // log.segments.ms: time kafka will wait until it closes a segment if not full (default: 1 week)
                // - smaller value:
                //  - more frequent log rollovers
                // Both depend on your use case.
                //
                // Segments have two indexes (files -- check in the data directory):
                // - An offset to position index -- this allows kafka to understand how to index into the segment
                // - A timestamp to offset index -- same as above but time to offset
                // This allows for constant time finds
                // NOTE: kafka only good for sequential reads and writes

                // Log cleanup policy
                // Policy 1: log.cleanup.policy=delete (default)
                // - Delete based on age of data (default is a week)
                // - based on size of the log file (default is -1 which means inf)
                // Policy 2: log.cleanup.policy=compact (this is the default for the __consumer_offsets topic)
                // - delete based on keys of your messages
                // - will delete duplicate keys after the active segment is committed
                // - infinite time and space retention
                // Deleting data from kafka allows you to:
                // - control the size of the data on the disk and remove obsolete data
                // - Overall, limit the maintenance work on the kafka cluster
                // This happens based on your segment configs
                // The cleaner checks ever 15secs (log.cleaner.backoff.ms)

                // Log cleanup policy: delete
                // log.retention.hours:
                // - num hours keep data (168 -- default 1 week)
                // log.retention.bytes:
                // - max size in bytes for each partition (default is -1 which means inf)

                // log cleanup policy: compaction
                // ensures that the partition contains at least the last known ue for a specific key
                // useful if we require snapshots
                // e.g. if seg0 contained key:value 1:100, 2:110, 3:113
                //      if segN contained key:value 2:210, 3:313
                //      Log compaction will keep 1:100, 2:210, 3:313, and delete 2:110, 3:113
                //      This means that there will be gaps in the offsets
                // Guarantees:
                // - consumption from the tail will not notice any difference
                // - ordering of messages is kept
                // - offsets are immutable, if offsets are missing after compaction they are just skipped
                // - deleted messages can bee seen by consumers for delete.retention.ms (default is 24 hrs)
                // - It doesn't prevent from pushing duplicate data
                //  - dedup happens when segment is committed
                // - log compaction thread could crash -- might require kafka restart (maybe be fixed in the future)
                // - cannot trigger log compaction through an api (yet)
                // min.compaction.lag.ms -- how long to wait before a message can be compacted (default: 0)
                // min.cleanup.dirty.ratio -- (default 0.5) higher => less, more efficient cleaning, lower => opposite

                // If all your in sync replicas (ISR) die and you're left with out of sync replicas:
                // - Wait for ISR to come back online (default)
                // - enable unclean.leader.election=true and producing to non ISR partitions
                //  - improved availability at the expense of losing data, because other msgs on ISR will be discarded when it comes back up
                //  - very dangerous setting, but acceptable in certain cases, e.g. metric and logging.


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

                // Kafka Streams:
                // - data processing and transformation library within Kafka
                //   - data transforms; data enrichment; fraud detection; monitoring and alerting
                // - Std Java app; no new separate cluster; exactly once capabilities; one record at a time (no batching);
                //   highly scalable.
                // - source -> connect cluster -> kafka cluster <-> streams app
                //     sink <- connect cluster <- kafka cluster
                // - Other contenders are Apache Spark, Flink or NiFi

                // Schema Registry in Kafka
                // Apache Avro as the chosen schema format
                //          |-> Schema Registry ->|
                // Producer ->      Kafka         -> Consumer
                //       Avro content          Avro content
                //
                // - Needs to be setup well
                // - Highly available
                // - Partially change the producer and consumer
                // - Can we use a different schema format e.g. json, protobuf, etc?

                // Partition count and replication factor
                // These params should be correct from the beginning
                // Changing the partition count during a topic lifecycle it will break your key ordering guarantee
                // Increasing the replicas could put more pressure on your cluster, making it unstable
                // Partitions:
                // Measure throughput -- it's usually around MB/s per partition
                // More partitions implies:
                // - Better parallelism, better throughput
                // - More consumers per consumer group
                // - Ability to leverage more brokers if the cluster is made up of lots of brokers
                // - BUT zookeeper is busier with elections
                // - BUT more files open in kafka
                // GUIDELINES (from lecturer):
                // - less than 6 brokers => 2x partitions per topic
                // - more than 12 brokers => 1x partitions per topic
                // - Adjust for num of consumers you need to run in parallel at peak throughput
                // - Adjust for producer throughput (inc if super high throughput or projected increase in the next 2 years)
                // - Test all cluster setups
                // - Don't create a topic with 1000 partitions; don't create topic with 2 partitions on a cluster with 20 brokers

                // Replication factor:
                // - at least 2, should be 3, at most 4
                // - Better resilience of your sys (N-1 brokers can fail)
                // - BUT Higher latency if acks=all
                // - BUT more disk space on your sys (50% more if RF is 3 instead of 2)
                // GUIDELINES
                // - set to 3 to start with; must have at least 3 brokers
                // - if replication performance is degrading the sys; get a better broker, never decrease the replication count
                // - Never set it to 1 in production

                // Cluster guidelines:
                // - Per broker no more than 2000 to 4000 partitions
                // - kafka cluster should have max 20,000 partitions across all brokers
                // - More brokers means more work for zookeeper
                // - If you need more brokers still, add more brokers instead
                // - More than 20,000 partitions in your cluster -- break it up into multiple kafka clusters
                // - Overall no topics with 1000 partitions to achieve high throughput, start at a reasonable number and test the performance

                // Topic names -- company should agree on the naming convention

                // Admin of Kafka
                // - Run them in different regions (not availability zones e.g. america and europe, just all in one)
                // - zookeeper replicas should be odd and running in all availability zones
                // - zookeeper and brokers should run on diff servers
                // - Monitoring needs to be implemented
                //   - exposed through JMX
                //   - Check for under replicated partitions -- may indicate high load on sys
                //   - Request Handlers -- overall utilization of an apache kafka broker
                //   - request timing -- how long it takes to reply to requests
                // - Requires good ops and a dedicated kafka admin
                //   - ops should be able to perform rolling restarts of brokers
                //   - update configs
                //   - rebalancing partitions
                //   - increasing replication factor
                //   - adding/replacing/removing a broker
                //   - upgrading a cluster with zero downtime
                // - alternative use kafka as a service

                // Secure:
                // - encryption: port 9093 with TLS
                // - auth: SASL or TLS
                // - ACL

                // Replicating clusters between availability zones:
                // Mirror Maker
                // Flink
                // uReplicator

                // Alternate ways of starting kafka
                // You can start kafka with the confluent cli (not for production use though)

                // Multiple replicas
                // To deploy multiple brokers you will need multiple server config files and then run
                // kafka-server-start.sh with each of the server configs -- remember to change the port
                // number.
                // When producing messages, you can connect to one broker which will connect to the whole
                // cluster (--broker-list 127.0.0.1:9092).
                // As the option denotes, you can give it a list of brokers, and the reason for that is
                // for resilience, in case the first broker goes down (--broker-list 127.0.0.1:9092,127.0.0.1:9093).

                // Helpful tools to run kafka in docker: https://github.com/conduktor/kafka-stack-docker-compose

                // Advertised Host (ADV_HOST)
                // The kafka client will connect to the broker with the public ip
                // The broker can then demand it connects to the ADV_HOST, which might be a private internal IP
                // The demand must be met otherwise the client can't communicate further with the broker.
                //
                // If the behaviour is that you wish the client to connect to the public ip address then
                // change advertised.listeners in the server.properties to the public ip address.

                // Changing the config of the topic through the CLI
                // To change the min insync replicas for the topic:
                // kafka-configs --zookeeper 127.0.0.1:2181 --entity-type topics --entity-name name-of-topic --add-config min.insync.replicas=2 --alter
                // To delete a config:
                // kafka-configs --zookeeper 127.0.0.1:2181 --entity-type topics --entity-name name-of-topic --delete-config min.insync.replicas --alter
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
        // TODO: Reuse this ObjectMapper
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
