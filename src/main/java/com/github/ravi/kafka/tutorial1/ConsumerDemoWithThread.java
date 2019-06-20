package com.github.ravi.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static sun.plugin2.main.client.LiveConnectSupport.shutdown;

public class ConsumerDemoWithThread {

    public static void main(String[] args) {

        new ConsumerDemoWithThread().run();

    }
    private  ConsumerDemoWithThread(){

    }
    private   void run(){
        Logger logger=LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        String bootstrapServers="127.0.0.1:9092";
        String groupId="my-sixth-application";
        String topic="first_topic";

        //latch for dealing with multiple thread
        CountDownLatch latch = new CountDownLatch(1);

        //create consumer runnable
        logger.info("Creating the consumer");
        Runnable myConsumerRunnable=new ConsumerThread(
                bootstrapServers,
                groupId,
                topic,
                latch
        );

        //start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("caught shutdown hook");
            ((ConsumerThread) myConsumerRunnable).shutdown();//we will do myConsumerRunnable.shutdown()
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }
        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted",e);
        }finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerThread implements Runnable{

        private CountDownLatch latch;
        private KafkaConsumer<String,String> consumer;
        private  Logger logger=LoggerFactory.getLogger(ConsumerThread.class.getName());

        public ConsumerThread(String bootstrapServers,
                              String groupId,
                              String topic,
                              CountDownLatch latch){

            this.latch=latch;


            //create consumer configs
            Properties properties=new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);

        /*The reason we do this is that basically when the producer takes a string, serializes it to bytes and sends it to Kafka, when Kafka sends these bytes right back to our consumer
        our consumer has to take these bytes and create a string from it and that process is called deserialization.This is why we provide a StringDeserializer .*/

            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");// we can have latest ( read from new message )or none


            //create consumer
            consumer = new KafkaConsumer<String, String>(properties);

            //subscribe consumer to our topic(s)
            consumer.subscribe(Arrays.asList(topic));
        }
        @Override
        public void run() {
            //poll for new data
            try{
            while (true){
                ConsumerRecords<String,String> records=
                        consumer.poll(Duration.ofMillis(100)); //new in kafka 2.0.0

                for (ConsumerRecord<String,String> record : records){
                    logger.info("Key: " + record.key() + ", Value: " + record.value());
                    logger.info("Partition : "+ record.partition() +", Offset: " + record.offset());
                }
            }

        }catch (WakeupException e){
                logger.info("Received shutdown signal!");
            }finally {
                consumer.close();
                //tell our main code we're done with the consumer
                latch.countDown();
            }
            }
        public void shutdown(){

            //wake up method is a special method to interrupt .poll,consumer.poll
            //It will throw exception and exception is called wakeup exception
            consumer.wakeup();


        }
    }
}
