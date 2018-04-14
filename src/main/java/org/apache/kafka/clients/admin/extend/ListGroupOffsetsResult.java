package org.apache.kafka.clients.admin.extend;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.internals.KafkaFutureImpl;

import java.util.Map;

/**
 * Created by hidden.zhu on 2018/4/13.
 */
@InterfaceStability.Evolving
public class ListGroupOffsetsResult {
    private final KafkaFutureImpl<Map<TopicPartition, Long>> future;

    public ListGroupOffsetsResult(KafkaFutureImpl<Map<TopicPartition, Long>> future) {
        this.future = future;
    }

    public KafkaFutureImpl<Map<TopicPartition, Long>> values(){
        return this.future;
    }
}
