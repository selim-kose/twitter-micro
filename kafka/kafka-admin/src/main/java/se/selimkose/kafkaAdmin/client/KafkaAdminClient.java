package se.selimkose.kafkaAdmin.client;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import se.selimkose.config.KafkaConfigData;
import se.selimkose.config.RetryConfigData;
import se.selimkose.kafkaAdmin.exception.KafkaClientException;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static java.lang.Thread.sleep;

@Component
public class KafkaAdminClient {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminClient.class);

    private final KafkaConfigData kafkaConfigData;
    private final RetryConfigData retryConfigData;
    private final AdminClient adminClient;
    private final RetryTemplate retryTemplate;

    private final WebClient webClient;


    public KafkaAdminClient(KafkaConfigData kafkaConfigData, RetryConfigData retryConfigData,
                            AdminClient adminClient, RetryTemplate retryTemplate, WebClient webClient) {
        this.kafkaConfigData = kafkaConfigData;
        this.retryConfigData = retryConfigData;
        this.adminClient = adminClient;
        this.retryTemplate = retryTemplate;
        this.webClient = webClient;

        log.info("Kafka Admin Client initialized with bootstrap servers: {}", kafkaConfigData.getBootstrapServers());
    }

    public void createTopics() {
        CreateTopicsResult createTopicsResult;

        try {
            createTopicsResult = retryTemplate.execute(this::doCreateTopics);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to create topics after max retries", t);
        }
        checkTopicsCreated();
    }

    public void checkTopicsCreated() {
        Collection<TopicListing> topics = getTopics();
        int retryCount = 1;
        Integer maxRetries = retryConfigData.getMaxAttempts();
        Integer multiplier = retryConfigData.getMultiplier().intValue();
        Long sleepTimeMs = retryConfigData.getSleepTimeMs();

        for(String topic: kafkaConfigData.getTopicNamesToCreate()) {
            while (!isTopicCreated(topics, topic)) {
                checkMaxRetries(retryCount++, maxRetries);
                sleep(sleepTimeMs);
                sleepTimeMs *= multiplier;

            }
        }
    }

    public void checkSchemaRegistry() {
        int retryCount = 1;
        Integer maxRetries = retryConfigData.getMaxAttempts();
        int multiplier = retryConfigData.getMultiplier().intValue();
        Long sleepTimeMs = retryConfigData.getSleepTimeMs();
        while(!getSchemaRegistryStatus().is2xxSuccessful()) {
            checkMaxRetries(retryCount++, maxRetries);
            sleep(sleepTimeMs);
            sleepTimeMs *= multiplier;
        }
    }

    private HttpStatus getSchemaRegistryStatus() {
        try {
            return (HttpStatus) webClient.method(HttpMethod.GET)
                    .uri(kafkaConfigData.getSchemaRegistryUrl())
                    .exchange()
                    .map(ClientResponse::statusCode)
                    .block();
        } catch (Exception e) {
           return HttpStatus.SERVICE_UNAVAILABLE;
        }
    }

    private void sleep(Long sleepTimeMs) {
        try {
            log.info("Sleeping for {} ms before retrying...", sleepTimeMs);
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Sleep interrupted", e);
            throw new KafkaClientException("Sleep interrupted while waiting to retry topic creation", e);
        }
    }

    private void checkMaxRetries(int retry, Integer maxRetries) {
        if( retry > maxRetries) {
            log.error("Max retries reached for creating topics, giving up after {} attempts", retry);
            throw new KafkaClientException("Max retries reached for creating topics");
        }
    }

    private boolean isTopicCreated(Collection<TopicListing> topics, String topicName) {
      if (topics == null ) {
            log.warn("No topics found, retrying...");
            return false;
      }
      return topics.stream()
                .anyMatch(topic -> topic.name().equals(topicName.trim()));
    }


    private CreateTopicsResult doCreateTopics(RetryContext retryContext) {
        List<String> topicNames = kafkaConfigData.getTopicNamesToCreate();
        log.info("Creating topics: {}, attempt {}", topicNames.size(), retryContext.getRetryCount());

        List<NewTopic> newTopics = topicNames.stream().map(topicName -> new NewTopic(
                        topicName.trim(),
                        kafkaConfigData.getNumberOfPartitions(),
                        kafkaConfigData.getReplicationFactor()))
                .toList();
        return adminClient.createTopics(newTopics);
    }

    private Collection<TopicListing> getTopics() {
        Collection<TopicListing> topics;

        try {
            topics = retryTemplate.execute(this::doGetTopics);
        } catch (Throwable t) {
            throw new KafkaClientException("Reached max retries for getting topics", t);
        }
        return topics;
    }

    private Collection<TopicListing> doGetTopics(RetryContext retryContext) throws ExecutionException, InterruptedException {
        log.info("Getting topics {}, attempt {}", kafkaConfigData.getTopicNamesToCreate().toArray(), retryContext.getRetryCount());
        Collection<TopicListing> topics = adminClient.listTopics().listings().get();

        if (topics != null) {
            topics.forEach(topic -> log.info("Found topic: {}", topic.name()));
        }

        log.info("Found {} topics", topics.size());
        return topics;
    }
}

