package com.hidden.custom.kafka.admin.model;

import lombok.Builder;
import lombok.Data;
import org.apache.kafka.common.TopicPartition;

import java.util.List;

/**
 * Created by hidden.zhu on 2018/4/10.
 */
@Data
@Builder
public class ConsumerSummary {
    private String consumerId;
    private String clientId;
    private String host;
    private List<TopicPartition> assignment;
}
