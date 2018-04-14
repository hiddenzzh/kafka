package org.apache.kafka.clients.admin.extend;

import org.apache.kafka.clients.admin.model.ConsumerGroupSummary;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * Created by hidden.zhu on 2018/4/12.
 */
@InterfaceStability.Evolving
public class DescribeConsumerGroupResult {
    private final KafkaFuture<ConsumerGroupSummary> future;

    public DescribeConsumerGroupResult(KafkaFuture<ConsumerGroupSummary> future) {
        this.future = future;
    }

    public KafkaFuture<ConsumerGroupSummary> values(){
        return future;
    }
}
