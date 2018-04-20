package com.hidden.custom.jmx;

import lombok.extern.slf4j.Slf4j;

import java.util.*;

import static java.util.stream.Collectors.toList;

/**
 * Created by hidden.zhu on 2018/4/19.
 */
@Slf4j
public class JmxManager {
    private static final int TRANSFER_B_TO_KB = 1024;

    private Map<String, JmxConn> jmxConnsMap = new HashMap<>();
    private Collection<String> kafkaJmxUrls;
    private String kafkaVersion;

    public JmxManager(Collection<String> kafkaJmxUrls, String kafkaVersion) {
        this.kafkaJmxUrls = kafkaJmxUrls;
        this.kafkaVersion = kafkaVersion;
    }

    public void init(){
        this.kafkaJmxUrls.forEach(url->{
            JmxConn jmxConn = null;
            try {
                log.info("init jmxConn with {}...", url);
                jmxConn = new JmxConn(url, this.kafkaVersion);
                jmxConnsMap.put(url, jmxConn);
                boolean connected = jmxConn.init();
                if (!connected) {
                    log.warn("init jmxConn error in ip:{}", url);
                }
            } catch (Exception e) {
                log.warn("init jmxConn error in ip:{}", url);
            }
        });
    }

    public void checkConnections(){
        jmxConnsMap.values().forEach(JmxConn::checkConnection);
        log.info("checking: jmx manager checks over");
    }

    public void closeConnections(){
        jmxConnsMap.values().forEach(JmxConn::closeConnection);
    }

    /**
     * 获取集群中针对某个topic的LEO
     */
    public Map<String, Long> getLogEndOffsets(String topic) {
        Map<String, Long> map = new HashMap<>();
        List<Map<String, Long>> leoList = jmxConnsMap.values().parallelStream()
                .map(jmxConn -> jmxConn.getPartitionLeo(topic))
                .collect(toList());
        leoList.forEach(leo -> leo.keySet().stream()
                .filter(partitionId -> !map.containsKey(partitionId)
                        || leo.get(partitionId) > map.get(partitionId))
                .forEach(partitionId -> map.put(partitionId, leo.get(partitionId))));
        return map;
    }

    /**
     * 获取日志起始Offset
     */
    public Map<String, Long> getLogStartOffsets(String topic){
        Map<String, Long> map = new HashMap<>();
        List<Map<String, Long>> lsoList = jmxConnsMap.values().parallelStream()
                .map(jmxConn -> jmxConn.getPartitionLogStartOffset(topic))
                .collect(toList());
        lsoList.forEach(lso -> lso.keySet().stream()
                .filter(partitionId -> !map.containsKey(partitionId)
                        || lso.get(partitionId) < map.get(partitionId))
                .forEach(partitionId -> map.put(partitionId, lso.get(partitionId))));
        return map;
    }

    /**
     * 消息流入速度（条/s）
     */
    public double getMsgIn(String topic) {
        return jmxConnsMap.values().parallelStream()
                .mapToDouble(jmxConn -> jmxConn.getMsgIn(topic))
                .sum();
    }

    /**
     * 消息流入速率（KB/s）
     */
    public double getBytesIn(String topic) {
        return jmxConnsMap.values().parallelStream()
                .mapToDouble(jmxConn -> jmxConn.getBytesIn(topic))
                .sum() / TRANSFER_B_TO_KB;
    }

    /**
     * 消息流出速率（KB/s）
     */
    public double getBytesOut(String topic) {
        return jmxConnsMap.values().parallelStream()
                .mapToDouble(jmxConn -> jmxConn.getBytesOut(topic))
                .sum() / TRANSFER_B_TO_KB;
    }

    /**
     * BytesRejected（KB/s）
     */
    public double getBytesRejected(String topic) {
        return jmxConnsMap.values().parallelStream()
                .mapToDouble(jmxConn -> jmxConn.getBytesRejected(topic))
                .sum() / TRANSFER_B_TO_KB;
    }

    /**
     * FailFetchRequests(次/s)
     */
    public double getFailedFetchRequests(String topic) {
        return jmxConnsMap.values().parallelStream()
                .mapToDouble(jmxConn -> jmxConn.getFailedFetchRequests(topic))
                .sum();
    }

    /**
     * FailedProduceRequests(次/s)
     */
    public double getFailedProduceRequests(String topic) {
        return jmxConnsMap.values().parallelStream()
                .mapToDouble(jmxConn -> jmxConn.getFailedProduceRequests(topic))
                .sum();
    }

    public Map<String, JmxConn> getJmxConnsMap() {
        return jmxConnsMap;
    }
}
