package com.coffeetechgaff.storm.spout;

import java.util.Map;

import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import com.coffeetechgaff.storm.utils.ExampleTopologyUtils;

/**
 * Common class to create kafka spouts
 * 
 * spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
 * spoutConfig.ignoreZkOffsets = true;
 * 
 * @author VivekSubedi
 *
 */
public class TopologyKafkaSpout{

	public KafkaSpout buildKafkaSpout(Map<String, Object> propertyMap, String topic){
		BrokerHosts hosts = new ZkHosts(propertyMap.get(ExampleTopologyUtils.KAFKAZOOKEEPER).toString());
		String zkRoot = propertyMap.get(ExampleTopologyUtils.KAFKAZKROOT).toString();
		SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, topic);
		// we ignore offsets so that the Zookeeper doesn't get out of sync
		spoutConfig.ignoreZkOffsets = true;
		return new KafkaSpout(spoutConfig);
	}
}
