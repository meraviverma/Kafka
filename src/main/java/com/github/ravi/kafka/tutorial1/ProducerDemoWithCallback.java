package com.github.ravi.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {
        System.out.println("Hello world!");

        //create a logger for my class

        final Logger logger=LoggerFactory.getLogger(ProducerDemoWithCallback.class);
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

        for(int i=0;i<10;i++) {
            //create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world" + Integer.toString(i));

            //send data - asynchronous
            // onCompletion run every time a record is being successfully send or there is exception
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                    //executes every time a record is successfully sent or an exception is thrown
                    if (e == null) {

                        //the record was successfully sent
                        logger.info("\nReceived new metadata \n" +
                                "topic:" + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp()
                        );
                    } else {

                        //e.printStackTrace();
                        logger.error("Error while producing ", e);
                    }
                }
            });
        }

        //flush data
        producer.flush();

        //flush and close producer
        producer.close();
    }
}
