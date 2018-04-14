package org.apache.kafka.server.policy;

import java.util.Map;

import org.apache.kafka.common.errors.PolicyViolationException;

public class PolicyDemo implements CreateTopicPolicy{
	
	public void configure(Map<String, ?> configs) {
		System.out.println("====================================================configure====");
		System.out.println(configs);
	}

	public void close() throws Exception {}

	public void validate(RequestMetadata requestMetadata)
			throws PolicyViolationException {
		System.out.println("====================================================validate=====");
		System.out.println(requestMetadata);
		if(requestMetadata.numPartitions()!=null || requestMetadata.replicationFactor()!=null){
			if(requestMetadata.numPartitions()< 5){
				throw new PolicyViolationException("Topic should have at least 5 partitions, received: "
						+ requestMetadata.numPartitions());
			}
			if(requestMetadata.replicationFactor()<= 1){
				throw new PolicyViolationException("Topic should have at least 2 replication factor, recevied: "
						+ requestMetadata.replicationFactor());
			}
		}
	}
}
