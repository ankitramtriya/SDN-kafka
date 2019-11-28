package com.example.kafka.rest;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@org.springframework.stereotype.Controller
public class Controller {

	@RequestMapping("/getdata")
	@ResponseBody
	public String getData()
	{
		KafkaConsumer<String,String> consumer = createConsumer("twitter_tweets");
		Logger logger = LoggerFactory.getLogger(Controller.class.getName());
		int i=2;
		String res = "";
		int j=0;
		while(i-->0){
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0
            
            System.out.print("count:"+records.count());
            j=records.count();
            for (ConsumerRecord<String, String> record : records){
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
                res+=record.value().toString();
            }
        }
            
		return res;
		
	}
	public static KafkaConsumer<String, String> createConsumer(String topic){

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100"); // disable auto commit of offsets

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));

        return consumer;

    }
}
