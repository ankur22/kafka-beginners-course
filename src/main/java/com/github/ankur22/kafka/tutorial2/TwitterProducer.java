package com.github.ankur22.kafka.tutorial2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    public static class Secrets {
        String consumerKey;
        String consumerSecret;
        String token;
        String secret;
        String[] terms;
        String kafkaHost;

        public String getKafkaHost() {
            return kafkaHost;
        }

        public void setKafkaHost(String kafkaHost) {
            this.kafkaHost = kafkaHost;
        }

        public String[] getTerms() {
            return terms;
        }

        public void setTerms(String[] terms) {
            this.terms = terms;
        }

        public String getConsumerKey() {
            return consumerKey;
        }

        public void setConsumerKey(String consumerKey) {
            this.consumerKey = consumerKey;
        }

        public String getConsumerSecret() {
            return consumerSecret;
        }

        public void setConsumerSecret(String consumerSecret) {
            this.consumerSecret = consumerSecret;
        }

        public String getToken() {
            return token;
        }

        public void setToken(String token) {
            this.token = token;
        }

        public String getSecret() {
            return secret;
        }

        public void setSecret(String secret) {
            this.secret = secret;
        }
    }

    public TwitterProducer() {
    }

    public static void main(String[] args) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        Secrets secrets = objectMapper.readValue(new File("secrets.json"), Secrets.class);

        new TwitterProducer().run(secrets);
    }

    public void run(Secrets secrets) {
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);

        // Create a twitter client
        Client hosebirdClient = createTwitterClient(secrets, msgQueue);

        // Attempts to establish a connection.
        hosebirdClient.connect();

        // Create a Kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer(secrets);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping application...");
            logger.info("shutting down client from twitter...");
            hosebirdClient.stop();
            logger.info("closing producer...");
            producer.close();
            logger.info("done!");
        }));

        // Loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!hosebirdClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                hosebirdClient.stop();
            }

            if (msg != null) {
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("something bad happened", e);
                        }
                    }
                });
            }
        }

        logger.info("End of application");
    }

    public Client createTwitterClient(Secrets secrets, BlockingQueue<String> msgQueue) {
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
//        List<Long> followings = Lists.newArrayList(1234L, 566788L); // Follow people
//        hosebirdEndpoint.followings(followings);
        List<String> terms = Lists.newArrayList(secrets.terms); // Follow terms
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(secrets.consumerKey, secrets.consumerSecret, secrets.token, secrets.secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();

        return hosebirdClient;
    }

    public KafkaProducer<String, String> createKafkaProducer(Secrets secrets) {
        // Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, secrets.kafkaHost);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // NOTE: If acks=all, then min.insync.replicas needs to be 2 or
        //       more based on how many replicas are present for the partition.
        //       When more replicas are down than min.insync.replicas then
        //       NotEnoughReplicasException is thrown.
        //       retries are set to max int (!) for kafka 2.1+. The backoff is
        //       set to 100ms in retry.backoff.ms. The total timeout is controlled
        //       with delivery.timeout.ms which is defaulted to 120000ms (2 mins).
        //
        // Warn: During retries messages could go out of order. This can be mitigated
        //       by setting max.in.flight.requests.per.connection to 1 (defaults to 5).
        //       This controls the parallelism of the connection from the producer.
        //       This may impact throughput.
        //
        // Idempotent
        // producer: To prevent duplicate messages being added to the topic.
        //           max.in.flight.requests is 5 (higher performance & keep ordering).
        //           Just set producerProps.put("enable.idempotence", true);

        // Create a safe producer - based on the notes above
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // Compression: Smaller; Faster to transfer (less latency); better throughput;
        //              better disk utilisation.
        //              producer and consumer needs to use a little CPU for compression.
        //              https://blog.cloudflare.com/squeezing-the-firehose/

        // Batch: Batches messages. One request per batch; better compression; less latency; more throughput.
        //        linger.ms (default 0ms) and batch.size (16 KB)
        //        Message bigger than batch.size will not be batched.
        //        Batch allocated per partition.
        //        Use Kafka Producer Metrics to monitor how well the batching is going.

        // High throughout settings - based on the notes above
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));

        // Create the Producer
        return new KafkaProducer<>(properties);
    }
}
