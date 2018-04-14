package com.hidden.custom.kafka.admin;

import com.hidden.custom.kafka.admin.model.ConsumerSummary;
import com.hidden.custom.kafka.admin.model.PartitionAssignmentState;
import kafka.admin.AdminClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;


/**
 * Created by hidden.zhu on 2018/4/10.
 */
@Slf4j
public class KafkaConsumerGroupCustomService {
    private static final String GROUP_ID = "ConsumerGroupID";

    private String brokerUrl;
    private AdminClient adminClient;
    private org.apache.kafka.clients.admin.AdminClient newAdminClient;
    private KafkaConsumer<String, String> consumer;

    public KafkaConsumerGroupCustomService(String brokerUrl) {
        this.brokerUrl = brokerUrl;
    }

    public void init() {
        this.adminClient = createAdminClient(this.brokerUrl);
        this.consumer = createNewConsumer(this.brokerUrl, GROUP_ID);
        this.newAdminClient = createNewAdminClient(this.brokerUrl);
    }

    public void close(){
        this.adminClient.close();
        this.consumer.close();
        this.newAdminClient.close();
    }

    public List<PartitionAssignmentState> collectGroupAssignment(String group){
        try {
            List<PartitionAssignmentState> list = collectGroupAssignment(this.adminClient, this.consumer, group);
            return list;
        } catch (Exception e) {
            log.error("error in collect group information", e);
        }
        return Collections.EMPTY_LIST;
    }

    public static List<PartitionAssignmentState> collectGroupAssignment(
            AdminClient adminClient, KafkaConsumer<String, String> consumer, String group) {
        AdminClient.ConsumerGroupSummary consumerGroupSummary
                = adminClient.describeConsumerGroup(group, 0);

        List<TopicPartition> assignedTopicPartitions = new ArrayList<>();
        List<PartitionAssignmentState> rowsWithConsumer = new ArrayList<>();

        scala.collection.immutable.List<AdminClient.ConsumerSummary> consumers
                = consumerGroupSummary.consumers().get();
        if (consumers.nonEmpty()) {
            scala.collection.immutable.Map<TopicPartition, Object> offsets
                    = adminClient.listGroupOffsets(group);
            if (offsets.nonEmpty()) {
                if (consumerGroupSummary.state().trim().equalsIgnoreCase("Stable")) {
                    List<ConsumerSummary> consumerList = changeToJavaList(consumers);
                    rowsWithConsumer = getRowsWithConsumer(consumerGroupSummary, offsets,
                            consumer, consumerList, assignedTopicPartitions, group);
                }
            }
            List<PartitionAssignmentState> rowsWithoutConsumer = getRowsWithoutConsumer(consumerGroupSummary,
                    offsets, consumer, assignedTopicPartitions, group);
            rowsWithConsumer.addAll(rowsWithoutConsumer);
        }
        return rowsWithConsumer;
    }

    private static Map<TopicPartition, Long> getLogEndOffsets(List<TopicPartition> list,
                                                              KafkaConsumer<String, String> consumer) {
        return consumer.endOffsets(list);
    }

    private static List<PartitionAssignmentState> getRowsWithConsumer(
            AdminClient.ConsumerGroupSummary consumerGroupSummary,
            scala.collection.immutable.Map<TopicPartition, Object> offsets,
            KafkaConsumer<String, String> consumer,
            List<ConsumerSummary> consumerList,
            List<TopicPartition> assignedTopicPartitions, String group) {
        List<PartitionAssignmentState> rowsWithConsumer = new ArrayList<>();
        for (ConsumerSummary cs : consumerList) {
            List<TopicPartition> tpList = cs.getAssignment();
            if (tpList.isEmpty()) {
                PartitionAssignmentState partitionAssignmentState = PartitionAssignmentState.builder()
                        .group(group).coordinator(consumerGroupSummary.coordinator())
                        .consumerId(cs.getConsumerId()).host(cs.getHost())
                        .clientId(cs.getClientId()).build();
                rowsWithConsumer.add(partitionAssignmentState);
            } else {
                Map<TopicPartition, Long> logEndOffsets = getLogEndOffsets(tpList, consumer);
                assignedTopicPartitions.addAll(tpList);
                List<PartitionAssignmentState> tempList = tpList.stream()
                        .sorted(comparing(TopicPartition::partition))
                        .map(tp->{
                            long offset = (Long) offsets.get(tp).get();
                            long leo = logEndOffsets.get(tp);
                            long lag = getLag(offset, leo);
                            return PartitionAssignmentState.builder()
                                    .group(group).coordinator(consumerGroupSummary.coordinator())
                                    .topic(tp.topic()).partition(tp.partition())
                                    .offset(offset).lag(lag).consumerId(cs.getConsumerId())
                                    .host(cs.getHost()).clientId(cs.getClientId()).logEndOffset(leo).build();
                        }).collect(toList());
                rowsWithConsumer.addAll(tempList);
            }
        }
        return rowsWithConsumer;
    }

    private static List<PartitionAssignmentState> getRowsWithoutConsumer(
            AdminClient.ConsumerGroupSummary consumerGroupSummary,
            scala.collection.immutable.Map<TopicPartition, Object> offsets,
            KafkaConsumer<String, String> consumer,
            List<TopicPartition> assignedTopicPartitions, String group) {
        List<TopicPartition> tpList = new ArrayList<>();
        offsets.keysIterator().foreach(tp -> tpList.add(tp));

        Map<TopicPartition, Long> logEndOffsets = getLogEndOffsets(tpList, consumer);
        return tpList.stream().
                filter(tp->!assignedTopicPartitions.contains(tp))
                .map(tp -> {
                    long leo = logEndOffsets.get(tp);
                    long offset = (Long) offsets.get(tp).get();
                    return PartitionAssignmentState.builder()
                            .group(group).coordinator(consumerGroupSummary.coordinator())
                            .topic(tp.topic()).partition(tp.partition()).offset(offset)
                            .logEndOffset(leo).lag(getLag(offset, leo)).build();
        }).collect(toList());
    }

    private static List<ConsumerSummary> changeToJavaList(
            scala.collection.immutable.List<AdminClient.ConsumerSummary> consumers) {
        List<ConsumerSummary> consumerList = new ArrayList<>();
        for (scala.collection.Iterator<AdminClient.ConsumerSummary> iterator = consumers.iterator();
             iterator.hasNext(); ) {
            AdminClient.ConsumerSummary acs = iterator.next();
            List<TopicPartition> tpl = new ArrayList<>();
            acs.assignment().iterator().foreach(tp -> tpl.add(tp));
            consumerList.add(ConsumerSummary.builder().consumerId(acs.consumerId())
                    .clientId(acs.clientId()).host(acs.host()).assignment(tpl).build());
        }
        consumerList.sort((a, b) -> b.getAssignment().size() - a.getAssignment().size());
        return consumerList;
    }

    private static long getLag(Long offset, Long leo) {
        long lag = leo - offset;
        return lag < 0 ? 0 : lag;
    }

    private static AdminClient createAdminClient(String brokerUrl) {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        AdminClient adminClient = AdminClient.create(props);
        return adminClient;
    }

    private static org.apache.kafka.clients.admin.AdminClient createNewAdminClient(String brokerUrl) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        org.apache.kafka.clients.admin.AdminClient kafkaAdminClient
                = org.apache.kafka.clients.admin.AdminClient.create(properties);
        return kafkaAdminClient;
    }

    private static KafkaConsumer<String, String> createNewConsumer(String brokerUrl, String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        return kafkaConsumer;
    }

    public static void printPasList(List<PartitionAssignmentState> list) {
        System.out.println(String.format("%-40s %-10s %-15s %-15s %-10s %-50s%-30s %s",
                "TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG", "CONSUMER-ID", "HOST", "CLIENT-ID"));
        list.forEach(item -> {
            System.out.println(String.format("%-40s %-10s %-15s %-15s %-10s %-50s%-30s %s",
                    item.getTopic(), item.getPartition(), item.getOffset(), item.getLogEndOffset(), item.getLag(),
                    Optional.ofNullable(item.getConsumerId()).orElse("-"),
                    Optional.ofNullable(item.getHost()).orElse("-"),
                    Optional.ofNullable(item.getClientId()).orElse("-")));
        });
    }

    /** just for test */
    public static void main(String[] args) {
        KafkaConsumerGroupCustomService service = new KafkaConsumerGroupCustomService("localhost:9092");
        service.init();
        List<PartitionAssignmentState> pasList = service.collectGroupAssignment("CONSUMER_GROUP_ID");
        printPasList(pasList);
        service.close();
    }
}
