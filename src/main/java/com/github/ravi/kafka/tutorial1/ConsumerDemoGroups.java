package com.github.ravi.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoGroups {
    public static void main(String[] args) {
        Logger logger=LoggerFactory.getLogger(ConsumerDemoGroups.class.getName());

        String bootstrapServers="127.0.0.1:9092";
        String groupId="my-fifth-application";
        String topic="first_topic";

        Properties properties=new Properties();

        //create consumer configs
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);

        /*The reason we do this is that basically when the producer takes a string, serializes it to bytes and sends it to Kafka, when Kafka sends these bytes right back to our consumer
        our consumer has to take these bytes and create a string from it and that process is called deserialization.This is why we provide a StringDeserializer .*/

        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");// we can have latest ( read from new message )or none

        //create consumer

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe consumer to our topic(s)
        consumer.subscribe(Collections.singleton(topic)); // we are saying that we are subscribing to only one topic. But we can subscribe to multiple topics.

        //consumer.subscribe(Arrays.asList("first_topic","second_topic")); //subscribe multiple topic

        //poll for new data
        while (true){
            ConsumerRecords<String,String> records=
                    consumer.poll(Duration.ofMillis(100)); //new in kafka 2.0.0

            for (ConsumerRecord<String,String> record : records){
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition : "+ record.partition() +", Offset: " + record.offset());
            }
        }



    }
}
