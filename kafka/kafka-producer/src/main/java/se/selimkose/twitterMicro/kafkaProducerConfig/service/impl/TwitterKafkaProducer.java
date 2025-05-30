package se.selimkose.twitterMicro.kafkaProducerConfig.service.impl;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import se.selimkose.kafka.avro.model.TwitterAvroModel;
import se.selimkose.twitterMicro.kafkaProducerConfig.service.KafkaProducer;

import java.util.concurrent.CompletableFuture;

@Service
public class TwitterKafkaProducer implements KafkaProducer<Long, TwitterAvroModel> {

    private static final Logger log = LoggerFactory.getLogger(TwitterKafkaProducer.class);
    private KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate;

    public TwitterKafkaProducer(KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PreDestroy
    public void close() {
        if (kafkaTemplate != null) {
            log.info("Closing Kafka producer");
            kafkaTemplate.destroy();
        }
    }

    @Override
    public void send(String topicName, Long key, TwitterAvroModel value) {
        log.info("Producing message to topic: {}, key: {}, value: {}", topicName, key, value);
      //  ListenableFuture<SendResult<Long, TwitterAvroModel>> kafkaResultFuture = kafkaTemplate.send(topicName, key, value);
        final CompletableFuture<SendResult<Long, TwitterAvroModel>> kafkaResultFuture =
                kafkaTemplate.send(topicName, key, value);

        addCallback(kafkaResultFuture, topicName, value);
    }

    private static void addCallback(CompletableFuture<SendResult<Long,TwitterAvroModel>> kafkaResultFuture,
                                    String topicName,
                                    TwitterAvroModel value) {
        kafkaResultFuture.whenComplete((result, throwable) -> {
            if (throwable != null) {
                log.error("Error sending message to topic: {}, value: {}, error: {}", topicName, value, throwable.getMessage());
            } else {
                log.info("Message sent successfully to topic: {}, value: {}, offset: {}",
                        topicName, value, result.getRecordMetadata().offset());
            }
        });
    }
}
