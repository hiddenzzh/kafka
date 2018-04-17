package com.hidden.custom.kafka.admin.model;

import lombok.Builder;
import lombok.Data;

/**
 * Created by hidden.zhu on 2018/4/10.
 */
@Data
@Builder
public class PartitionAssignmentState {
    private String group;
    private Node coordinator;
    private String topic;
    private int partition;
    private long offset;
    private long lag;
    private String consumerId;
    private String host;
    private String clientId;
    private long logEndOffset;

    @Data
    public static class Node{
        public int id;
        public String idString;
        public String host;
        public int port;
        public String rack;
    }
}
