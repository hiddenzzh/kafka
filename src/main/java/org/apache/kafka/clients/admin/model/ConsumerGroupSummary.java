package org.apache.kafka.clients.admin.model;

import lombok.Builder;
import lombok.Data;
import org.apache.kafka.common.Node;

import java.util.List;

/**
 * Created by hidden.zhu on 2018/4/10.
 */
@Data
@Builder
public class ConsumerGroupSummary {
    private String state;
    private String assignmentStrategy;
    private List<ConsumerSummary> consumers;
    private Node coordinator;
}
