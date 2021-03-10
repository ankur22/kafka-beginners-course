package tutorial3;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
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

    public static void main(String[] args) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        Config config = objectMapper.readValue(new File("consumer-config.json"), Config.class);

        RestHighLevelClient client = createClient(config);

        String jsonString = "{\"foo\":\"bar\"}";

        IndexRequest indexRequest = new IndexRequest("twitter", "tweets").source(jsonString, XContentType.JSON);

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        String id = indexResponse.getId();
        logger.info(id);

        client.close();
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
}
