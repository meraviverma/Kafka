package com.github.ravi.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

//https://kafka.apache.org/documentation/#consumerconfigs
public class ProducerDemo {
    public static void main(String[] args) {
        System.out.println("Hello world!");

        String bootstrapServers="127.0.0.1:9092";

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //This technique was used earlier

        //properties.setProperty("bootstrap.servers",bootstrapServers);
        //properties.setProperty("key.serializer",StringSerializer.class.getName());
        //properties.setProperty("value.serializer",StringSerializer.class.getName());

        //create the producer

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        //create producer record

        ProducerRecord<String,String> record=new ProducerRecord<String, String>("first_topic","hello world");

        //send data - asynchronous
        producer.send(record);

        //flush data
        producer.flush();

        //flush and close producer
        producer.close();
    }
}
