package org.apache.kafka.clients.admin.app;

import com.hidden.custom.kafka.admin.ConsumerGroupUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.extend.DescribeConsumerGroupResult;
import org.apache.kafka.clients.admin.extend.ListGroupOffsetsResult;
import org.apache.kafka.clients.admin.model.ConsumerGroupSummary;
import org.apache.kafka.clients.admin.model.ConsumerSummary;
import org.apache.kafka.clients.admin.model.PartitionAssignmentState;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

/**
 * Created by hidden.zhu on 2018/4/13.
 */
@Slf4j
public class KafkaConsumerGroupService{
    private static final String GROUP_ID = "ConsumerGroupID";
    private static final int TIMEOUT = 5;//5s

    private String brokerUrl;
    private AdminClient newAdminClient;
    private KafkaConsumer<String, String> consumer;

    public KafkaConsumerGroupService(String brokerUrl) {
        this.brokerUrl = brokerUrl;
    }

    public void init(){
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        this.newAdminClient = AdminClient.create(props);
        this.consumer = ConsumerGroupUtils.createNewConsumer(this.brokerUrl, GROUP_ID);
    }

    public void close(){
        if (this.newAdminClient != null) {
            this.newAdminClient.close();
        }
        if (this.consumer != null) {
            this.consumer.close();
        }
    }


    public List<PartitionAssignmentState> collectGroupAssignment(String group) {
        List<PartitionAssignmentState> list = new ArrayList<>();
        try {
            list = collectGroupAssignment(this.newAdminClient, this.consumer, group);
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            log.error("error in collect group information", e);
        }
        return list;
    }

    private List<PartitionAssignmentState> collectGroupAssignment(AdminClient adminClient,
                                                                        KafkaConsumer<String, String> consumer,
                                                                        String group)
            throws ExecutionException, InterruptedException, TimeoutException {
        DescribeConsumerGroupResult describeConsumerGroupResult = adminClient.describeConsumerGroup(group);
        ConsumerGroupSummary consumerGroupSummary = describeConsumerGroupResult.values().get(TIMEOUT, TimeUnit.SECONDS);

        List<TopicPartition> assignedTopicPartitions = new ArrayList<>();
        List<PartitionAssignmentState> rowsWithConsumer = new ArrayList<>();

        List<ConsumerSummary> consumers = consumerGroupSummary.getConsumers();
        if (consumers != null && !consumers.isEmpty()) {
            ListGroupOffsetsResult listGroupOffsetsResult = adminClient.listGroupOffsets(group);
            Map<TopicPartition, Long> offsets = listGroupOffsetsResult.values().get(TIMEOUT, TimeUnit.SECONDS);
            if (offsets != null && !offsets.isEmpty()) {
                if (consumerGroupSummary.getState().trim().equalsIgnoreCase("Stable")) {
                    rowsWithConsumer = getRowsWithConsumer(consumerGroupSummary, offsets, consumer, consumers,
                            assignedTopicPartitions, group);
                }
            }
            List<PartitionAssignmentState> rowsWithoutConsumer = getRowsWithoutConsumer(consumerGroupSummary,
                    offsets,consumer,assignedTopicPartitions,group);
            rowsWithConsumer.addAll(rowsWithoutConsumer);
        }
        return rowsWithConsumer;
    }

    private static List<PartitionAssignmentState> getRowsWithConsumer(ConsumerGroupSummary consumerGroupSummary,
                                                                      Map<TopicPartition, Long> offsets,
                                                                      KafkaConsumer<String, String> consumer,
                                                                      List<ConsumerSummary> consumerList,
                                                                      List<TopicPartition> assignedTopicPartitions,
                                                                      String group) {
        List<PartitionAssignmentState> rowsWithConsumer = new ArrayList<>();
        consumerList.forEach(cs->{
            List<TopicPartition> tpList = cs.getAssignment();
            if (tpList != null && tpList.isEmpty()) {
                rowsWithConsumer.add(PartitionAssignmentState.builder().group(group)
                        .coordinator(consumerGroupSummary.getCoordinator())
                        .consumerId(cs.getConsumerId()).host(cs.getHost())
                        .clientId(cs.getClientId()).build());
            } else {
                Map<TopicPartition, Long> logEndOffsets = consumer.endOffsets(tpList);
                assignedTopicPartitions.addAll(tpList);
                List<PartitionAssignmentState> tempList = tpList.stream()
                        .sorted(comparing(TopicPartition::partition))
                        .map(tp->{
                            long offset = offsets.get(tp);
                            long leo = logEndOffsets.get(tp);
                            long lag = getLag(offset, leo);
                            return PartitionAssignmentState.builder()
                                    .group(group).coordinator(consumerGroupSummary.getCoordinator())
                                    .topic(tp.topic()).partition(tp.partition()).offset(offset)
                                    .lag(lag).consumerId(cs.getConsumerId()).host(cs.getHost())
                                    .clientId(cs.getClientId()).logEndOffset(leo).build();
                        }).collect(toList());
                rowsWithConsumer.addAll(tempList);
            }
        });
        return rowsWithConsumer;
    }

    private static List<PartitionAssignmentState> getRowsWithoutConsumer(ConsumerGroupSummary consumerGroupSummary,
                                                                         Map<TopicPartition,Long> offsets,
                                                                         KafkaConsumer<String,String> consumer,
                                                                         List<TopicPartition> assignedTopicPartitions,
                                                                         String group) {
        Set<TopicPartition> tpSet = offsets.keySet();
        Map<TopicPartition, Long> logEndOffsets = consumer.endOffsets(tpSet);
        return tpSet.stream()
                .filter(tp -> !assignedTopicPartitions.contains(tp))
                .map(tp -> {
                    long leo = logEndOffsets.get(tp);
                    long offset = offsets.get(tp);
                    return PartitionAssignmentState.builder().group(group)
                            .coordinator(consumerGroupSummary.getCoordinator())
                            .topic(tp.topic()).partition(tp.partition()).offset(offset)
                            .logEndOffset(leo).lag(getLag(offset, leo)).build();
                }).collect(toList());
    }

    private static long getLag(Long offset, Long leo) {
        long lag = leo - offset;
        return lag < 0 ? 0 : lag;
    }

}
