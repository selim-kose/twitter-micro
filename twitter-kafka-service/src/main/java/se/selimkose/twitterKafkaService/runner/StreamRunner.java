package se.selimkose.twitterKafkaService.runner;


import twitter4j.TwitterException;

public interface StreamRunner {
    void start() throws TwitterException;
}
