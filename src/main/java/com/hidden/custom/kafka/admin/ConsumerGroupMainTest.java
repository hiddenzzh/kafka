package com.hidden.custom.kafka.admin;

import org.apache.kafka.clients.admin.app.KafkaConsumerGroupService;
import org.apache.kafka.clients.admin.model.PartitionAssignmentState;

import java.util.List;

/**
 * Created by hidden.zhu on 2018/4/14.
 */
public class ConsumerGroupMainTest {
    public static void main(String[] args) {
        testKafkaConsumerGroupCustomService();
        System.out.println("----------------------------------------------");
        testKafkaConsumerGroupService();
    }


    public static void testKafkaConsumerGroupService(){
        KafkaConsumerGroupService service = new KafkaConsumerGroupService("localhost:9092");
        service.init();
        List<PartitionAssignmentState> pasList = service.collectGroupAssignment("CONSUMER_GROUP_ID");
        ConsumerGroupUtils.printPasList(pasList);
        service.close();
    }

    public static void testKafkaConsumerGroupCustomService(){
        KafkaConsumerGroupCustomService service = new KafkaConsumerGroupCustomService("localhost:9092");
        service.init();
        List<PartitionAssignmentState> pasList = service.collectGroupAssignment("CONSUMER_GROUP_ID");
        ConsumerGroupUtils.printPasList(pasList);
        service.close();
    }
}
