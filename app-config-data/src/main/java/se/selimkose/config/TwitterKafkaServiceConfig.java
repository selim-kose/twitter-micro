package se.selimkose.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Data
@Configuration
@ConfigurationProperties(prefix = "twitter-kafka-service")
public class TwitterKafkaServiceConfig {
   private List<String> twitterKeywords;
   private String welcomeMessage;

   private Boolean enableMockTweets;
   private Long mockSleepMs;
   private Integer mockMinTweetLength;
   private Integer mockMaxTweetLength;
}
