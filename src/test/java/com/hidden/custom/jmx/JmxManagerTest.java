package com.hidden.custom.jmx;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;


/**
 * Created by hidden.zhu on 2018/4/20.
 */
public class JmxManagerTest {
    private static final String topic = "topic-test1";
    private static JmxManager jmxManager;

    @BeforeClass
    public static void beforeClass() {
        Collection<String> jmxUrl = new ArrayList<>();
        jmxUrl.add("10.xxx.xxx.2:9999");
        jmxUrl.add("10.xxx.xxx.3:9999");
        jmxUrl.add("10.xxx.xxx.4:9999");
        String kafkaVersion = "1.0.0";
        jmxManager = new JmxManager(jmxUrl, kafkaVersion);
        jmxManager.init();
    }

    @AfterClass
    public static void afterClass(){
        jmxManager.closeConnections();
    }

    @Test
    public void getLogEndOffsets() throws Exception {
        System.out.println(jmxManager.getLogEndOffsets(topic));
    }

    @Test
    public void getLogStartOffsets() throws Exception {

    }

    @Test
    public void getMsgIn() throws Exception {

    }

    @Test
    public void getBytesIn() throws Exception {

    }

    @Test
    public void getBytesOut() throws Exception {

    }

    @Test
    public void getBytesRejected() throws Exception {

    }

    @Test
    public void getFailedFetchRequests() throws Exception {

    }

    @Test
    public void getFailedProduceRequests() throws Exception {

    }

    @Test
    public void getJmxConnsMap() throws Exception {

    }

}