package org.apache.kafka.clients.admin.app;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.extend.DescribeConsumerGroupResult;
import org.apache.kafka.clients.admin.extend.ListGroupOffsetsResult;
import org.apache.kafka.clients.admin.model.ConsumerGroupSummary;
import org.apache.kafka.clients.admin.model.ConsumerSummary;
import org.apache.kafka.clients.admin.model.PartitionAssignmentState;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by hidden.zhu on 2018/4/13.
 */
public class KafkaConsumerGroupService {
    private static final int TIMEOUT = 5;//5s

    private String brokerUrl;
    private AdminClient newAdminClient;

    public KafkaConsumerGroupService(String brokerUrl) {
        this.brokerUrl = brokerUrl;
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        newAdminClient = AdminClient.create(props);
    }


    public static List<PartitionAssignmentState> collectGroupAssignment(AdminClient adminClient, String group)
            throws ExecutionException, InterruptedException, TimeoutException {
        DescribeConsumerGroupResult describeConsumerGroupResult = adminClient.describeConsumerGroup(group);
        ConsumerGroupSummary consumerGroupSummary = describeConsumerGroupResult.values().get(TIMEOUT, TimeUnit.SECONDS);

        List<TopicPartition> assignedTopicPartitions = new ArrayList<>();
        List<PartitionAssignmentState> rowsWithConsumer = new ArrayList<>();

        List<ConsumerSummary> consumers = consumerGroupSummary.getConsumers();
        if (consumers != null && !consumers.isEmpty()) {
            ListGroupOffsetsResult listGroupOffsetsResult = adminClient.listGroupOffsets(group);
            Map<TopicPartition, Long> offsets = listGroupOffsetsResult.values().get(TIMEOUT,TimeUnit.SECONDS);
            if (offsets != null && !offsets.isEmpty()) {
                if (consumerGroupSummary.getState().trim().equalsIgnoreCase("Stable")) {
                    rowsWithConsumer = getRowsWithConsumer();
                }
            }
            List<PartitionAssignmentState> rowsWithoutConsumer = getRowsWithoutConsumer();
            rowsWithConsumer.addAll(rowsWithoutConsumer);
        }
        return rowsWithConsumer;
    }

    private static List<PartitionAssignmentState> getRowsWithoutConsumer() {
        return null;
    }

    private static List<PartitionAssignmentState> getRowsWithConsumer() {
        return null;
    }


    public static void main(String[] args) {

    }

}
