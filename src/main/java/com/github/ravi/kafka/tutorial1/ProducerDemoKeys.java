package com.github.ravi.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.out.println("Hello world!");

        //create a logger for my class

        final Logger logger=LoggerFactory.getLogger(ProducerDemoKeys.class);
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

            String topic="first_topic";
            String value="hello world" + Integer.toString(i);
            String key="id_" + Integer.toString(i);

            ProducerRecord<String, String> record = new ProducerRecord(topic,value,key);

            logger.info("Key: " + key);
            //id_0 is going to partition 1
            //id_1 partition 0



            //send data - asynchronous
            // onCompletion run every time a record is being successfully send or there is exception
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                    if (e == null) {

                        logger.info("Received new metadata \n" +
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
            }).get(); //block the .send() to make it synchronous - don't do this in production!
        }

        //flush data
        producer.flush();

        //flush and close producer
        producer.close();
    }
}
