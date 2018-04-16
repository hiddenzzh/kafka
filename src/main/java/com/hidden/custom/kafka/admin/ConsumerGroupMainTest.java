package com.hidden.custom.kafka.admin;

import org.apache.kafka.clients.admin.app.KafkaConsumerGroupService;
import org.apache.kafka.clients.admin.model.PartitionAssignmentState;

import java.util.List;

/**
 * Created by hidden.zhu on 2018/4/14.
 */
public class ConsumerGroupMainTest {
    public static void main(String[] args) {
        String brokerUrl = "localhost:9092";
        String group = "CONSUMER_GROUP_ID";
        testKafkaConsumerGroupCustomService(brokerUrl,group);
        System.out.println("----------------------------------------------");
        testKafkaConsumerGroupService(brokerUrl,group);
    }


    public static void testKafkaConsumerGroupService(String brokerUrl, String group){
        KafkaConsumerGroupService service = new KafkaConsumerGroupService(brokerUrl);
        service.init();
        List<PartitionAssignmentState> pasList = service.collectGroupAssignment(group);
        ConsumerGroupUtils.printPasList(pasList);
        service.close();
    }

    public static void testKafkaConsumerGroupCustomService(String brokerUrl, String group){
        KafkaConsumerGroupCustomService service = new KafkaConsumerGroupCustomService(brokerUrl);
        service.init();
        List<PartitionAssignmentState> pasList = service.collectGroupAssignment(group);
        ConsumerGroupUtils.printPasList(pasList);
        service.close();
    }
}
