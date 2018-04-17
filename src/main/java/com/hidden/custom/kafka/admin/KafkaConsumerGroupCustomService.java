package com.hidden.custom.kafka.admin;

import kafka.admin.AdminClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.model.ConsumerSummary;
import org.apache.kafka.clients.admin.model.PartitionAssignmentState;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import scala.collection.JavaConverters;

import java.util.*;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;


/**
 * Created by hidden.zhu on 2018/4/10.
 */
@Slf4j
public class KafkaConsumerGroupCustomService{
    private static final String GROUP_ID = "ConsumerGroupID";

    private String brokerUrl;
    private AdminClient adminClient;
    private KafkaConsumer<String, String> consumer;

    public KafkaConsumerGroupCustomService(String brokerUrl) {
        this.brokerUrl = brokerUrl;
    }

    public void init() {
        this.adminClient = createAdminClient(this.brokerUrl);
        this.consumer = ConsumerGroupUtils.createNewConsumer(this.brokerUrl, GROUP_ID);
    }

    public void close(){
        if (this.adminClient != null) {
            this.adminClient.close();
        }
        if (this.consumer != null) {
            this.consumer.close();
        }
    }

    public List<PartitionAssignmentState> collectGroupAssignment(String group){
        List<PartitionAssignmentState> list = new ArrayList<>();
        try {
            list = collectGroupAssignment(this.adminClient, this.consumer, group);
        } catch (Exception e) {
            log.error("error in collect group information", e);
        }
        return list;
    }

    public List<PartitionAssignmentState> collectGroupAssignment(
            AdminClient adminClient, KafkaConsumer<String, String> consumer,
            String group) {
        //1. 获取consumer group的基本信息，包括CONSUMER-ID、HOST、
        // CLIENT-ID以及TopicPartition信息
        AdminClient.ConsumerGroupSummary consumerGroupSummary
                = adminClient.describeConsumerGroup(group, 0);
        List<TopicPartition> assignedTopicPartitions = new ArrayList<>();
        List<PartitionAssignmentState> rowsWithConsumer = new ArrayList<>();
        scala.collection.immutable.List<AdminClient.ConsumerSummary> consumers
                = consumerGroupSummary.consumers().get();
        if (consumers != null) {
            //2. 获取各个分区(Partition)的对应的消费位移CURRENT-OFFSET
            scala.collection.immutable.Map<TopicPartition, Object> offsets
                    = adminClient.listGroupOffsets(group);
            if (offsets.nonEmpty()) {
                String state = consumerGroupSummary.state();
                // 3. 还有一个状态是Dead表示"group"对应的consumer group不存在
                if (state.equals("Stable") || state.equals("Empty")
                        || state.equals("PreparingRebalance")
                        || state.equals("AwaitingSync")) {
                    List<ConsumerSummary> consumerList = changeToJavaList(consumers);
                    // 4. 获取当前有消费者的消费信息，即包含CONSUMER-ID、HOST、CLIENT-ID
                    rowsWithConsumer = getRowsWithConsumer(consumerGroupSummary, offsets,
                            consumer, consumerList, assignedTopicPartitions, group);
                }
            }
            //5. 获取当前没有消费者的消费信息
            List<PartitionAssignmentState> rowsWithoutConsumer =
                    getRowsWithoutConsumer(consumerGroupSummary,
                    offsets, consumer, assignedTopicPartitions, group);
            //6. 合并结果
            rowsWithConsumer.addAll(rowsWithoutConsumer);
        }
        return rowsWithConsumer;
    }

    static Map<TopicPartition, Long> getLogEndOffsets(List<TopicPartition> list,
                                                              KafkaConsumer<String, String> consumer) {
        return consumer.endOffsets(list);
    }

    public List<PartitionAssignmentState> getRowsWithConsumer(
            AdminClient.ConsumerGroupSummary consumerGroupSummary,
            scala.collection.immutable.Map<TopicPartition, Object> offsets,
            KafkaConsumer<String, String> consumer,
            List<ConsumerSummary> consumerList,
            List<TopicPartition> assignedTopicPartitions, String group) {
        List<PartitionAssignmentState> rowsWithConsumer = new ArrayList<>();
        for (ConsumerSummary cs : consumerList) {
            List<TopicPartition> tpList = cs.getAssignment();
            if (tpList != null && tpList.isEmpty()) {
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
                        .map(tp -> {
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

    private List<PartitionAssignmentState> getRowsWithoutConsumer(
            AdminClient.ConsumerGroupSummary consumerGroupSummary,
            scala.collection.immutable.Map<TopicPartition, Object> offsets,
            KafkaConsumer<String, String> consumer,
            List<TopicPartition> assignedTopicPartitions, String group) {
        List<TopicPartition> tpList = new ArrayList<>();
        offsets.keysIterator().foreach(tp -> tpList.add(tp));

        Map<TopicPartition, Long> logEndOffsets = getLogEndOffsets(tpList, consumer);
        return tpList.stream()
                .filter(tp -> !assignedTopicPartitions.contains(tp))
                .sorted((tp1, tp2) -> tp1.partition() - tp2.partition())
                .map(tp -> {
                    long leo = logEndOffsets.get(tp);
                    long offset = (Long) offsets.get(tp).get();
                    return PartitionAssignmentState.builder()
                            .group(group).coordinator(consumerGroupSummary.coordinator())
                            .topic(tp.topic()).partition(tp.partition()).offset(offset)
                            .logEndOffset(leo).lag(getLag(offset, leo)).build();
                }).collect(toList());
    }

    private List<ConsumerSummary> changeToJavaList(
            scala.collection.immutable.List<AdminClient.ConsumerSummary> consumers) {
        List<ConsumerSummary> consumerList = JavaConverters.seqAsJavaList(consumers).stream()
                .map(cs -> {
                    List<TopicPartition> tpl = new ArrayList<>();
                    cs.assignment().iterator().foreach(tp -> tpl.add(tp));
                    return ConsumerSummary.builder().consumerId(cs.consumerId())
                            .clientId(cs.clientId()).host(cs.host()).assignment(tpl).build();
                }).collect(toList());
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
}
