package com.github.ravi.kafka.tutorial2;

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

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

//https://github.com/twitter/hbc
public class TwitterProducer {

    Logger logger= LoggerFactory.getLogger(TwitterProducer.class.getName());

    String consumerKey="yDdiDHSWypxYRIGd8vSzbqCL5";
    String consumerSecret="BXxEgSktiQY8uvLZ5UEpAnoTtXBhgbiHNBcfUcMpKEVcZlqSWM";
    String token="869182616204869632-JgjFvgjYOOwTAVi3f253vqdiov5X2V2";
    String secret="x8VgbN9yCWIkV08ntciRcPXSNiem1ndlyjnN69KtkefX0";

    //constructor
    public TwitterProducer(){

    }
    public static void main(String[] args){
        //System.out.println("hello");
        new TwitterProducer().run();
    }
    //run method
    public void run(){

        logger.info("setup");

        //1) we have a message queue
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        //2)We have twitter client
        //create a twitter client
        Client client=createTwitterClient(msgQueue);


        //3)And then client connects
        //loop to send tweets to kafka
        // Attempts to establish a connection.
        client.connect();

        //commented
        //create a kafka producer
       KafkaProducer<String,String> producer=createkafkaProducer();

        //4)Till client is not doen we are pulling the message
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null ;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
           if(msg != null){
            logger.info(msg);
            //commented
            //will send a producer record. We will have a new callback on completion
           producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
               @Override
               public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e!=null){
                    logger.error("someting Bad happened",e);
                }
               }
           });
           }
        }

        logger.info("End of Application");
    }



    public Client createTwitterClient(BlockingQueue<String> msgQueue){


        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        //we have to follow either peoples or terms. We will follow terms here.Comment following
        //List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("SachinOpensAgain","sachinopensagain");
        //hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey,consumerSecret, token, secret);

        //Create a client

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
               // .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        return hosebirdClient;

    }
//commented
    public KafkaProducer<String,String> createkafkaProducer(){
        String bootstrapServers="127.0.0.1:9092";

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }
}
