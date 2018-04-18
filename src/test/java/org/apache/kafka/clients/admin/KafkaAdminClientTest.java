package org.apache.kafka.clients.admin;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Created by hidden.zhu on 2018/4/18.
 */
public class KafkaAdminClientTest {
    private static final String OLD_TOPIC = "topic-test1";
    private static final String NEW_TOPIC = "topic-test2";
    private static final String brokerUrl = "localhost:9092";

    private static AdminClient adminClient;

    @BeforeClass
    public static void beforeClass(){
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        adminClient = AdminClient.create(properties);
    }

    @AfterClass
    public static void afterClass(){
        adminClient.close();
    }

    @Test
    public void createTopics() {
        NewTopic newTopic = new NewTopic(NEW_TOPIC,4, (short) 1);
        Collection<NewTopic> newTopicList = new ArrayList<>();
        newTopicList.add(newTopic);
        CreateTopicsResult result = adminClient.createTopics(newTopicList);
        try {
            result.all().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void deleteTopic() {
        adminClient.deleteTopics(Arrays.asList(NEW_TOPIC));
    }

    @Test
    public void listTopics() throws ExecutionException, InterruptedException {
        ListTopicsResult result = adminClient.listTopics();
        Collection<TopicListing> list = result.listings().get();
        System.out.println(list);
    }

    @Test
    public void listTopicsIncludeInternal() throws ExecutionException, InterruptedException {
        ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
        listTopicsOptions.listInternal(true);
        ListTopicsResult result = adminClient.listTopics(listTopicsOptions);
        Collection<TopicListing> list = result.listings().get();
        System.out.println(list);
    }

    @Test
    public void describeTopics() throws ExecutionException, InterruptedException {
        DescribeTopicsResult result = adminClient.describeTopics(Arrays.asList(OLD_TOPIC));
        Map<String, TopicDescription> map = result.all().get();
        System.out.println(map);
    }

    @Test
    public void describeCluster() throws ExecutionException, InterruptedException {
        DescribeClusterResult result = adminClient.describeCluster();
        Collection<Node> nodes = result.nodes().get();
        Node controller = result.controller().get();
        String clusterId = result.clusterId().get();
        System.out.println(nodes);
        System.out.println(controller);
        System.out.println(clusterId);
    }

    @Test
    public void describeConfigs() throws ExecutionException, InterruptedException {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, OLD_TOPIC);
        DescribeConfigsResult result = adminClient.describeConfigs(Arrays.asList(configResource));
        Map<ConfigResource, Config> map = result.all().get();
        map.forEach((resouce,config)->{
            System.out.println(resouce + ":");
            config.entries().forEach(System.out::println);
        });
    }

    @Test
    public void describeLogDirs() throws ExecutionException, InterruptedException {
        DescribeLogDirsResult result = adminClient.describeLogDirs(Arrays.asList(0));
        Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> map = result.all().get();
        Map<String, DescribeLogDirsResponse.LogDirInfo> details = map.get(0);
        details.forEach((a1,logDirInfo)->{
            System.out.println(a1+">>>>>");
            logDirInfo.replicaInfos.forEach((tp,replicaInfo)->{
                System.out.print(tp + " == ");
                System.out.println(replicaInfo);
            });
        });
    }
}