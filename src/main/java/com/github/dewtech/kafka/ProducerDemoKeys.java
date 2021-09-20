package com.github.dewtech.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootstrapServers = "127.0.0.1:9092";
        //Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //Create a producer record
        for(int i=0; i<10; i++){
            String topic ="first_topic";
            String value = "hello world" + Integer.toString(i);
            String key = "id_"+ Integer.toString(i);

            ProducerRecord<String ,String> record = new ProducerRecord<>(topic,key,value);

            logger.info("Key: " + key); // log the key

            //Send data - asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // execute every time a record is sucessfully sent or an exception is thrown
                    if(e ==null) {
                        //the record was successfully sent
                        logger.info("Received new metada. \n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition:" + recordMetadata.partition() + "\n" +
                                "Offset:" + recordMetadata.offset() + "\n" +
                                "Timestamp:" + recordMetadata.timestamp());

                    }else{
                        logger.error("Error while producing", e);
                    }
                }
            }).get(); // Block the .send() to make it synchronous - don't do this in production!
        }


        // flush data
        producer.flush();

        //Close conection
        producer.close();
    }
}
