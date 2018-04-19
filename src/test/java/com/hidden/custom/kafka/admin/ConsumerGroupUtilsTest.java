package com.hidden.custom.kafka.admin;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;


/**
 * Created by hidden.zhu on 2018/4/19.
 */
public class ConsumerGroupUtilsTest {
    @Test
    public void createNewConsumer() throws Exception {
        String brokerUrl = "localhost:9092";
        String groupId = "ITSNOTMATTER";
        String topic = "topic-test1";
        KafkaConsumer consumer = ConsumerGroupUtils.createNewConsumer(brokerUrl, groupId);
        Collection<TopicPartition> collection = new ArrayList<>();
        collection.add(new TopicPartition(topic, 0));
        collection.add(new TopicPartition(topic, 1));
        collection.add(new TopicPartition(topic, 2));
        collection.add(new TopicPartition(topic, 3));
        System.out.println(consumer.endOffsets(collection));
    }

    @Test
    public void printPasList() throws Exception {

    }

}