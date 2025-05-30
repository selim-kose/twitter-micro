package se.selimkose.twitterMicro.kafkaProducerConfig;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import se.selimkose.config.KafkaConfigData;
import se.selimkose.config.KafkaProducerConfigData;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig <K extends Serializable, V extends SpecificRecordBase> {

    private final KafkaProducerConfigData kafkaProducerConfigData;
    private final KafkaConfigData kafkaConfigData;

    public KafkaProducerConfig(KafkaProducerConfigData kafkaProducerConfigData, KafkaConfigData kafkaConfigData) {
        this.kafkaProducerConfigData = kafkaProducerConfigData;
        this.kafkaConfigData = kafkaConfigData;
    }

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfigData.getBootstrapServers());
        props.put(kafkaConfigData.getSchemaRegistryUrlKey(), kafkaConfigData.getSchemaRegistryUrl());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaProducerConfigData.getKeySerializer());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaProducerConfigData.getValueSerializer());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, kafkaProducerConfigData.getCompressionType());
        props.put(ProducerConfig.ACKS_CONFIG, kafkaProducerConfigData.getAcks());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, kafkaProducerConfigData.getBatchSize() *
                kafkaProducerConfigData.getBatchSizeBoostFactor());
        props.put(ProducerConfig.LINGER_MS_CONFIG, kafkaProducerConfigData.getLingerMs());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, kafkaProducerConfigData.getRequestTimeoutMs());
        props.put(ProducerConfig.RETRIES_CONFIG, kafkaProducerConfigData.getRetryCount());

        return props;
    }

}
