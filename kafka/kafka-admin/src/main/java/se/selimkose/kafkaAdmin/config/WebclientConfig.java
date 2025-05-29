package se.selimkose.kafkaAdmin.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
public class WebclientConfig {

    @Bean
    WebClient webClient() {
        return WebClient.builder()
                .baseUrl("http://localhost:8080") // Replace with your actual base URL
                .build();
    }

}
