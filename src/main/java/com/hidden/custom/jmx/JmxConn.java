package com.hidden.custom.jmx;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 只针对Kafka1.0.0版本做数据采集，其他的版本暂不考虑
 * Created by hidden.zhu on 2018/4/19.
 */
@Slf4j
public class JmxConn {
    private JMXConnector connector;
    private MBeanServerConnection connection;
    @Getter
    private String jmxUrl;
    private String kafkaVersion;

    private volatile ConnectStatus connectStatus = ConnectStatus.CONNECT_FAILED;

    public JmxConn(String ipAndPort, String kafkaVersion) {
        this.jmxUrl = "service:jmx:rmi:///jndi/rmi://" + ipAndPort + "/jmxrmi";
        this.kafkaVersion = kafkaVersion;
    }

    public Boolean init() {
        try {
            JMXServiceURL serviceURL = new JMXServiceURL(jmxUrl);
            connector = JMXConnectorFactory.connect(serviceURL, null);
            connection = connector.getMBeanServerConnection();
            if (connection == null) {
                log.error("get jmx connection return null");
                connectStatus = ConnectStatus.CONNECT_FAILED;
                return false;
            }
        } catch (Exception e) {
            log.error("init jmx connection failed", e);
            connectStatus = ConnectStatus.CONNECT_FAILED;
            return false;
        }
        connectStatus = ConnectStatus.CONNECT_OK;
        return true;
    }

    private Object getAttribute(String objName, String objAttr) {
        ObjectName objectName;
        try {
            objectName = new ObjectName(objName);
        } catch (MalformedObjectNameException e) {
            log.error("invalid object name {}!", objName, e);
            return null;
        }
        return getAttribute(objectName, objAttr);
    }

    private Object getAttribute(ObjectName objName, String objAttr) {
        try {
            return connection.getAttribute(objName, objAttr);
        } catch (Exception e) {
            log.warn("get attribute error in [{}] with attribute: [{}] in [{}].", objName, objAttr, this.jmxUrl);
        }
        return null;
    }

    /**
     * 获取LEO
     */
    public Map<String, Long> getPartitionLeo(String topic) {
        Set<ObjectName> objectNames = getPartitionLeoObjects(topic);
        if (objectNames == null) {
            return null;
        }
        return getPartitionValues(objectNames);
    }

    /**
     * 获取LogStartOffset
     */
    public Map<String, Long> getPartitionLogStartOffset(String topic) {
        Set<ObjectName> objectNames = getPartitionLogStartOffsetObjects(topic);
        if (objectNames == null) {
            return null;
        }
        return getPartitionValues(objectNames);
    }

    private Map<String, Long> getPartitionValues(Set<ObjectName> objectNames) {
        Map<String, Long> map = new HashMap<>();
        for (ObjectName objectName : objectNames) {
            String partitionId = getPartitionId(objectName);
            Object value = getAttribute(objectName, "Value");
            if (value != null) {
                map.put(partitionId, (Long) value);
            }
        }
        return map;
    }

    private Set<ObjectName> getPartitionLeoObjects(String topic) {
        String objName;
        if (this.kafkaVersion.startsWith("0.8.1")) {
            objName = "\"kafka.log\":type=\"Log\",name=\"" + topic + "-*-LogEndOffset\"";
        } else {
            objName = "kafka.log:type=Log,name=LogEndOffset,topic=" + topic + ",partition=*";
        }
        Set<ObjectName> set = null;
        try {
            set = this.connection.queryNames(new ObjectName(objName), null);
        } catch (Exception e) {
            log.error("get leo object error.", e);
        }
        return set;
    }

    private Set<ObjectName> getPartitionLogStartOffsetObjects(String topic) {
        String objName;
        if (this.kafkaVersion.startsWith("0.8.1")) {
            objName = "\"kafka.log\":type=\"Log\",name=\"" + topic + "-*-LogStartOffset\"";
        } else {
            objName = "kafka.log:type=Log,name=LogStartOffset,topic=" + topic + ",partition=*";
        }
        Set<ObjectName> set = null;
        try {
            set = this.connection.queryNames(new ObjectName(objName), null);
        } catch (Exception e) {
            log.error("get leo object error.", e);
        }
        return set;
    }

    private String getPartitionId(ObjectName objectName) {
        if (this.kafkaVersion.startsWith("0.8.1")) {
            String temp1 = objectName.getKeyProperty("name");
            int to = temp1.lastIndexOf("-LogEndOffset");
            String temp2 = temp1.substring(0, to);
            int from = temp2.lastIndexOf("-" + 1);
            return temp1.substring(from, to);
        } else {
            return objectName.getKeyProperty("partition");
        }
    }

    /**
     * 消息流入速度（条/s）
     */
    public double getMsgIn(String topic) {
        String s;
        if (this.kafkaVersion.startsWith("0.8.1")) {
            s = "\"kafka.server\":type=\"BrokerTopicMetrics\",name=\"" + topic + "-MessagesInPerSec\"";
        } else {
            s = "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=" + topic;
        }
        Object val = getAttribute(s, "OneMinuteRate");
        if (val != null) return (double) (Double) val;
        return 0;
    }

    /**
     * 消息流入速率（B/s）
     */
    public double getBytesIn(String topic) {
        String s;
        if (this.kafkaVersion.startsWith("0.8.1")) {
            s = "\"kafka.server\":type=\"BrokerTopicMetrics\",name=\"" + topic + "-BytesInPerSec\"";
        } else {
            s = "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic=" + topic;
        }
        Object val = getAttribute(s, "OneMinuteRate");
        if (val != null) return (double) (Double) val;
        return 0;
    }

    /**
     * 消息流出速率（B/s）
     */
    public double getBytesOut(String topic) {
        String s;
        if (this.kafkaVersion.startsWith("0.8.1")) {
            s = "\"kafka.server\":type=\"BrokerTopicMetrics\",name=\"" + topic + "-BytesOutPerSec\"";
        } else {
            s = "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec,topic=" + topic;
        }
        Object val = getAttribute(s, "OneMinuteRate");
        if (val != null) return (double) (Double) val;
        return 0;
    }

    /**
     * BytesRejected(B/s)
     */
    public double getBytesRejected(String topic) {
        String s;
        if (this.kafkaVersion.startsWith("0.8.1")) {
            s = "\"kafka.server\":type=\"BrokerTopicMetrics\",name=\"" + topic + "-BytesRejectedPerSec\"";
        } else {
            s = "kafka.server:type=BrokerTopicMetrics,name=BytesRejectedPerSec,topic=" + topic;
        }
        Object val = getAttribute(s, "OneMinuteRate");
        if (val != null) return (double) (Double) val;
        return 0;
    }

    /**
     * FailFetchRequests(次/s)
     */
    public double getFailedFetchRequests(String topic) {
        String s;
        if (this.kafkaVersion.startsWith("0.8.1")) {
            s = "\"kafka.server\":type=\"BrokerTopicMetrics\",name=\"" + topic + "-FailedFetchRequestsPerSec\"";
        } else {
            s = "kafka.server:type=BrokerTopicMetrics,name=FailedFetchRequestsPerSec,topic=" + topic;
        }
        Object val = getAttribute(s, "OneMinuteRate");
        if (val != null) return (double) (Double) val;
        return 0;
    }

    /**
     * FailedProduceRequests(次/s)
     */
    public double getFailedProduceRequests(String topic) {
        String s;
        if (this.kafkaVersion.startsWith("0.8.1")) {
            s = "\"kafka.server\":type=\"BrokerTopicMetrics\",name=\"" + topic + "-FailedProduceRequestsPerSec\"";
        } else {
            s = "kafka.server:type=BrokerTopicMetrics,name=FailedProduceRequestsPerSec,topic=" + topic;
        }
        Object val = getAttribute(s, "OneMinuteRate");
        if (val != null) return (double) (Double) val;
        return 0;
    }

    public long getActiveControllerCount() {
        String s;
        if (this.kafkaVersion.startsWith("0.8.1")) {
            throw new UnsupportedOperationException("can not support kafka version before 0.8.2.x");
        } else {
            s = "kafka.controller:type=KafkaController,name=ActiveControllerCount";
        }
        Object val = getAttribute(s, "Value");
        if (val != null) return (int) (Integer) val;
        return 0;
    }

    public double getMsgIn() {
        String s;
        if (this.kafkaVersion.startsWith("0.8.1")) {
            throw new UnsupportedOperationException("can not support kafka version before 0.8.2.x");
        } else {
            s = "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec";
        }
        Object val = getAttribute(s, "OneMinuteRate");
        if (val != null) return (double) (Double) val;
        return 0;
    }

    /**
     * broker重启之后就需要重新计数了
     *
     * @return
     */
    public long getMsgInCount() {
        String s;
        if (this.kafkaVersion.startsWith("0.8.1")) {
            throw new UnsupportedOperationException("can not support kafka version before 0.8.2.x");
        } else {
            s = "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec";
        }
        Object val = getAttribute(s, "Count");
        if (val != null) return (long) (Long) val;
        return 0;
    }

    public double getBytesIn() {
        String s;
        if (this.kafkaVersion.startsWith("0.8.1")) {
            throw new UnsupportedOperationException("can not support kafka version before 0.8.2.x");
        } else {
            s = "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec";
        }
        Object val = getAttribute(s, "OneMinuteRate");
        if (val != null) return (double) (Double) val;
        return 0;
    }

    public double getBytesOut() {
        String s;
        if (this.kafkaVersion.startsWith("0.8.1")) {
            throw new UnsupportedOperationException("can not support kafka version before 0.8.2.x");
        } else {
            s = "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec";
        }
        Object val = getAttribute(s, "OneMinuteRate");
        if (val != null) return (double) (Double) val;
        return 0;
    }

    public double getFailedFetchRequests() {
        String s;
        if (this.kafkaVersion.startsWith("0.8.1")) {
            throw new UnsupportedOperationException("can not support kafka version before 0.8.2.x");
        } else {
            s = "kafka.server:type=BrokerTopicMetrics,name=FailedFetchRequestsPerSec";
        }
        Object val = getAttribute(s, "OneMinuteRate");
        if (val != null) return (double) (Double) val;
        return 0;
    }

    public double getFailedProduceRequests() {
        String s;
        if (this.kafkaVersion.startsWith("0.8.1")) {
            throw new UnsupportedOperationException("can not support kafka version before 0.8.2.x");
        } else {
            s = "kafka.server:type=BrokerTopicMetrics,name=FailedProduceRequestsPerSec";
        }
        Object val = getAttribute(s, "OneMinuteRate");
        if (val != null) return (double) (Double) val;
        return 0;
    }

    public double getBytesRejected() {
        String s;
        if (this.kafkaVersion.startsWith("0.8.1")) {
            throw new UnsupportedOperationException("can not support kafka version before 0.8.2.x");
        } else {
            s = "kafka.server:type=BrokerTopicMetrics,name=BytesRejectedPerSec";
        }
        Object val = getAttribute(s, "OneMinuteRate");
        if (val != null) return (double) (Double) val;
        return 0;
    }

    public double getRequestAvgIdle() {
        String s;
        if (this.kafkaVersion.startsWith("0.8.1")) {
            throw new UnsupportedOperationException("can not support kafka version before 0.8.2.x");
        } else {
            s = "kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent";
        }
        Object val = getAttribute(s, "OneMinuteRate");
        if (val != null) return (double) (Double) val;
        return 0;
    }

    public double getCpuUsed() {
        String s;
        if (this.kafkaVersion.startsWith("0.8.1")) {
            throw new UnsupportedOperationException("can not support kafka version before 0.8.2.x");
        } else {
            s = "java.lang:type=OperatingSystem";
        }
        Object val = getAttribute(s, "SystemCpuLoad");
        if (val != null) return (double) (Double) val;
        return 0;
    }

    public long getMemFree() {
        String s;
        if (this.kafkaVersion.startsWith("0.8.1")) {
            throw new UnsupportedOperationException("can not support kafka version before 0.8.2.x");
        } else {
            s = "java.lang:type=OperatingSystem";
        }
        Object val = getAttribute(s, "FreePhysicalMemorySize");
        if (val != null) return (long) (Long) val;
        return 0;
    }

    public long getMemTotal() {
        String s;
        if (this.kafkaVersion.startsWith("0.8.1")) {
            throw new UnsupportedOperationException("can not support kafka version before 0.8.2.x");
        } else {
            s = "java.lang:type=OperatingSystem";
        }
        Object val = getAttribute(s, "TotalPhysicalMemorySize");
        if (val != null) return (long) (Long) val;
        return 0;
    }

    public long getFdUsed() {
        String s;
        if (this.kafkaVersion.startsWith("0.8.1")) {
            throw new UnsupportedOperationException("can not support kafka version before 0.8.2.x");
        } else {
            s = "java.lang:type=OperatingSystem";
        }
        Object val = getAttribute(s, "OpenFileDescriptorCount");
        if (val != null) return (long) (Long) val;
        return 0;
    }

    public long getFdTotal() {
        String s;
        if (this.kafkaVersion.startsWith("0.8.1")) {
            throw new UnsupportedOperationException("can not support kafka version before 0.8.2.x");
        } else {
            s = "java.lang:type=OperatingSystem";
        }
        Object val = getAttribute(s, "MaxFileDescriptorCount");
        if (val != null) return (long) (Long) val;
        return 0;
    }

    public long getLeaderCount() {
        String s;
        if (this.kafkaVersion.startsWith("0.8.1")) {
            throw new UnsupportedOperationException("can not support kafka version before 0.8.2.x");
        } else {
            s = "kafka.server:type=ReplicaManager,name=LeaderCount";
        }
        Object val = getAttribute(s, "Value");
        if (val != null) return (long) (Integer) val;
        return 0;
    }

    public long getPartitionCount() {
        String s;
        if (this.kafkaVersion.startsWith("0.8.1")) {
            throw new UnsupportedOperationException("can not support kafka version before 0.8.2.x");
        } else {
            s = "kafka.server:type=ReplicaManager,name=PartitionCount";
        }
        Object val = getAttribute(s, "Value");
        if (val != null) return (long) (Integer) val;
        return 0;
    }

    public long getUnderReplicatedPartitions() {
        String s;
        if (this.kafkaVersion.startsWith("0.8.1")) {
            throw new UnsupportedOperationException("can not support kafka version before 0.8.2.x");
        } else {
            s = "kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions";
        }
        Object val = getAttribute(s, "Value");
        if (val != null) return (long) (Integer) val;
        return 0;
    }

    public void checkConnection() {
        log.info("checking jmx connection url={} and status={}", jmxUrl, connectStatus);
        if (connectStatus == ConnectStatus.CONNECT_FAILED) {
            init();
        }
    }

    public void closeConnection() {
        try {
            if (connection != null) {
                connector.close();
            }
            if (connector != null) {
                connector.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public enum ConnectStatus {
        CONNECT_OK,
        CONNECT_FAILED
    }
}
