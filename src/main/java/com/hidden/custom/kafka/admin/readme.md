

### 步骤
1. 启动Kafka服务
2. topic: topic-test1 
3. consumerg group id: CONSUMER_GROUP_ID
4. 启动KafkaProducer往topic中发送消息；启动KafkaConsumer消费，指定相应的consumerg group id
5. 保证编译环境在Java8以上，并运行KafkaConsumerGroupCustomService的main方法得到下面的运行结果


### 运行效果
```
TOPIC                                    PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG        CONSUMER-ID                                       HOST                           CLIENT-ID
topic-test1                              0          183             183             0          CLIENT_ID-86686bcb-a4b8-49b8-b1da-fc42709b35f9    /192.168.92.1                  CLIENT_ID
topic-test1                              1          183             183             0          CLIENT_ID-86686bcb-a4b8-49b8-b1da-fc42709b35f9    /192.168.92.1                  CLIENT_ID
topic-test1                              2          184             184             0          CLIENT_ID-b6038e35-ac5a-40c1-8451-236f1a1d35f3    /192.168.92.1                  CLIENT_ID
topic-test1                              3          183             183             0          CLIENT_ID-b6038e35-ac5a-40c1-8451-236f1a1d35f3    /192.168.92.1                  CLIENT_ID
```

### 自定义
自定义自己的程序，对程序中的List&lt;PartitionAssignmentState>做相应的处理。

```
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
