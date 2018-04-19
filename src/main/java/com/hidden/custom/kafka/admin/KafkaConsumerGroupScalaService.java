package com.hidden.custom.kafka.admin;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.scala.DefaultScalaModule;
import com.hidden.custom.kafka.admin.model.PartitionAssignmentState;
import kafka.admin.ConsumerGroupCommand;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Created by hidden.zhu on 2018/4/17.
 */
public class KafkaConsumerGroupScalaService {
    public static final String brokers = "localhost:9092";
    public static final String groupId = "CONSUMER_GROUP_ID";

    /**
     * 注意要点：
     * 1. PartitionAssignmentState中的coordinator是Node类型，这个类型需要自定义，Kafka原生的会报错。
     * 2. 反序列化时Node会有一个empty的属性不识别，解决方案参考代码中的步骤2.
     */
    public static void main(String[] args) throws IOException {
        String[] agrs = {"--describe", "--bootstrap-server", brokers, "--group", groupId};
        ConsumerGroupCommand.ConsumerGroupCommandOptions options =
                new ConsumerGroupCommand.ConsumerGroupCommandOptions(agrs);
        ConsumerGroupCommand.KafkaConsumerGroupService kafkaConsumerGroupService =
                new ConsumerGroupCommand.KafkaConsumerGroupService(options);

        ObjectMapper mapper = new ObjectMapper();
        //1. 使用jackson-module-scala_2.12
        mapper.registerModule(new DefaultScalaModule());
        //2. 反序列化时忽略对象不存在的属性
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        //3. 将Scala对象序列化成JSON字符串
        String source = mapper.writeValueAsString(kafkaConsumerGroupService.describeGroup()._2.get());
        //4. 将JSON字符串反序列化成Java对象
        List<PartitionAssignmentState> target = mapper.readValue(source,
                getCollectionType(mapper,List.class,PartitionAssignmentState.class));
        //5. 排序
        target.sort((o1, o2) -> o1.getPartition() - o2.getPartition());
        //6. 打印
        printPasList(target);
    }

    public static JavaType getCollectionType(ObjectMapper mapper,
                                             Class<?> collectionClass,
                                             Class<?>... elementClasses) {
        return mapper.getTypeFactory().constructParametricType(collectionClass, elementClasses);
    }

    public static void printPasList(List<PartitionAssignmentState> list) {
        System.out.println(String.format("%-40s %-10s %-15s %-15s %-10s %-50s%-30s %s",
                "TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG", "CONSUMER-ID", "HOST", "CLIENT-ID"));
        list.forEach(item -> {
            System.out.println(String.format("%-40s %-10s %-15s %-15s %-10s %-50s%-30s %s",
                    item.getTopic(), item.getPartition(), item.getOffset(), item.getLogEndOffset(), item.getLag(),
                    Optional.ofNullable(item.getConsumerId()).orElse("-"),
                    Optional.ofNullable(item.getHost()).orElse("-"),
                    Optional.ofNullable(item.getClientId()).orElse("-")));
        });
    }
}
