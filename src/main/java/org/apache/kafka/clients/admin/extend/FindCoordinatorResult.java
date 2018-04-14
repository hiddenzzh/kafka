package org.apache.kafka.clients.admin.extend;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * Created by hidden.zhu on 2018/4/13.
 */
@InterfaceStability.Evolving
public class FindCoordinatorResult {
    private final KafkaFuture<Node> future;

    public FindCoordinatorResult(KafkaFuture<Node> future) {
        this.future = future;
    }

    public KafkaFuture<Node> values(){
        return future;
    }
}
