package com.hidden.custom.jmx;

import com.hidden.custom.jmx.JmxConn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.stream.Collectors.toList;

/**
 * Created by hidden.zhu on 2018/4/19.
 */
public class JmxManager {
    private static final int TRANSFER_B_TO_KB = 1024;
    private List<String> jmxUrlList;
    private String kafkaVersion;

    private List<JmxConn> jmxConnList = new ArrayList<>();
    private ExecutorService executorService;

    public JmxManager(List<String> jmxUrlList, String kafkaVersion) {
        this.jmxUrlList = jmxUrlList;
        this.kafkaVersion = kafkaVersion;
    }

    public void init(){
        this.jmxUrlList.forEach(url->{
            JmxConn jmxConn = new JmxConn(url, this.kafkaVersion);
            boolean isOk = jmxConn.init();
            if (isOk) {
                jmxConnList.add(jmxConn);
            }
        });
        executorService = Executors.newFixedThreadPool(Math.min(jmxUrlList.size(),
                Runtime.getRuntime().availableProcessors()));
    }

    public void close(){
        executorService.shutdown();
        this.jmxConnList.forEach(JmxConn::closeConnection);
    }

    public Map<String, Long> getLeo(String topic) {
        Map<String, Long> map = new HashMap<>();
        List<CompletableFuture<Map<String, Long>>> leoCompletableFutures = this.jmxConnList.stream()
                .map(jmxConn -> CompletableFuture
                        .supplyAsync(() -> jmxConn.getPartitionLeo(topic), executorService))
                .collect(toList());
        List<Map<String, Long>> leoList = leoCompletableFutures.stream()
                .map(CompletableFuture::join)
                .collect(toList());
        leoList.forEach(leo -> leo.keySet().stream()
                .filter(partitionId -> !map.containsKey(partitionId) || leo.get(partitionId) > map.get(partitionId))
                .forEach(partitionId -> map.put(partitionId, leo.get(partitionId))));
        return map;
    }

    public Map<String, Long> getLogStartOffsets(String topic) {
        Map<String, Long> map = new HashMap<>();
        List<Map<String, Long>> lsoList = this.jmxConnList.parallelStream()
                .map(jmxConn -> jmxConn.getPartitionLogStartOffset(topic))
                .collect(toList());
        lsoList.forEach(leo -> leo.keySet().stream()
                .filter(partitionId -> !map.containsKey(partitionId) || leo.get(partitionId) < map.get(partitionId))
                .forEach(partitionId -> map.put(partitionId, leo.get(partitionId))));
        return map;
    }
























}
