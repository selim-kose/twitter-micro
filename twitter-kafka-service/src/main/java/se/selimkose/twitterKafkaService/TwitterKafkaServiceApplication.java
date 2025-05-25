package se.selimkose.twitterKafkaService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import se.selimkose.twitterKafkaService.config.TwitterKafkaServiceConfig;
import se.selimkose.twitterKafkaService.runner.StreamRunner;


@SpringBootApplication
public class TwitterKafkaServiceApplication implements CommandLineRunner {
    private static final Logger logger = LoggerFactory.getLogger(TwitterKafkaServiceApplication.class);
    private final TwitterKafkaServiceConfig twitterKafkaServiceConfig;
    private final StreamRunner streamRunner;

    public TwitterKafkaServiceApplication(TwitterKafkaServiceConfig twitterKafkaServiceConfig, StreamRunner runner) {
        this.twitterKafkaServiceConfig = twitterKafkaServiceConfig;
        this.streamRunner = runner;
    }

    public static void main(String[] args) {
        org.springframework.boot.SpringApplication.run(TwitterKafkaServiceApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        logger.info("Twitter Kafka Service is starting...");
        logger.info(twitterKafkaServiceConfig.getTwitterKeywords().toString());
        logger.info(twitterKafkaServiceConfig.getWelcomeMessage());
        streamRunner.start();
    }
}
