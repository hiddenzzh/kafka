
### Preview
两种方式实现如同 bin/kafka-consumer-group.sh --describe --bootstrap-server localhost:9092 --group CONSUMER_GROUP_ID的效果
其中：
 - localhost:9092代表kafka broker的地址
 - CONSUMER_GROUP_ID代表consumer group id
 
### Introduction
#### 第一种实现
参见com.hidden.custom.kafka.admin.KafkaConsumerGroupCustomService，主要是通过调用Scala语言编写的kafka.admin.ConsumerGroupCommand.scala文件中的KafkaConsumerGroupService中的方法来实现。
#### 第二种实现
参见org.apache.kafka.clients.admin.app.KafkaConsumerGroupService，主要是通过扩展KafkaAdminClient来实现。

### Demo
运行环境：JDK8
执行ConsumerGroupMainTest效果参考如下：

```
TOPIC                                    PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG        CONSUMER-ID                                       HOST                           CLIENT-ID
topic-test1                              0          1648            1648            0          CLIENT_ID-e2d41f8d-dbd2-4f0e-9239-efacb55c6261    /192.168.92.1                  CLIENT_ID
topic-test1                              1          1648            1648            0          CLIENT_ID-e2d41f8d-dbd2-4f0e-9239-efacb55c6261    /192.168.92.1                  CLIENT_ID
topic-test1                              2          1648            1648            0          CLIENT_ID-e2d41f8d-dbd2-4f0e-9239-efacb55c6261    /192.168.92.1                  CLIENT_ID
topic-test1                              3          1648            1648            0          CLIENT_ID-e2d41f8d-dbd2-4f0e-9239-efacb55c6261    /192.168.92.1                  CLIENT_ID
----------------------------------------------
TOPIC                                    PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG        CONSUMER-ID                                       HOST                           CLIENT-ID
topic-test1                              0          1648            1648            0          CLIENT_ID-e2d41f8d-dbd2-4f0e-9239-efacb55c6261    /192.168.92.1                  CLIENT_ID
topic-test1                              1          1648            1648            0          CLIENT_ID-e2d41f8d-dbd2-4f0e-9239-efacb55c6261    /192.168.92.1                  CLIENT_ID
topic-test1                              2          1648            1648            0          CLIENT_ID-e2d41f8d-dbd2-4f0e-9239-efacb55c6261    /192.168.92.1                  CLIENT_ID
topic-test1                              3          1648            1648            0          CLIENT_ID-e2d41f8d-dbd2-4f0e-9239-efacb55c6261    /192.168.92.1                  CLIENT_ID
```

### 扩展
自定义自己的程序，获取List&lt;PartitionAssignmentState>做相应的处理即可。PartitionAssignmentState的字段参考如下：
```
@Builder @Data
public class PartitionAssignmentState {
    private String group;// consumer group id
    private Node coordinator;// consumer coordinator
    private String topic;
    private int partition;
    private long offset;
    private long lag;
    private String consumerId;
    private String host;
    private String clientId;
    private long logEndOffset;
}
```
