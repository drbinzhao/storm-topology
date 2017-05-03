package com.coffeetechgaff.storm.topology;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.InputDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coffeetechgaff.storm.bolt.AlgorithmBolt;
import com.coffeetechgaff.storm.bolt.DataBolt;
import com.coffeetechgaff.storm.bolt.GraphBolt;
import com.coffeetechgaff.storm.redis.Redis;
import com.coffeetechgaff.storm.redis.RedisClient;
import com.coffeetechgaff.storm.spout.TopologyKafkaSpout;
import com.coffeetechgaff.storm.utils.ExampleTopologyUtils;

/**
 * Topology that connects all spouts and bolts and executes in a sequential
 * manner.
 * 
 * @author VivekSubedi
 *
 */
public class ExampleTopology{

	private static final Logger logger = LoggerFactory.getLogger(ExampleTopology.class);

	private static final String KAFKADATASPOUTID = "dataKafkaSpout";
	private static final String KAFKAALGORITHMSPOUTID = "algorithmKafkaSpout";
	private static final String ALGORITHMBOLTID = "algorithmBolt";
	private static final String DATABOLTID = "dataeBolt";
	private static final String GRAPHBOLTID = "graphBolt";

	private TopologyBuilder topologyBuilder = new TopologyBuilder();
	private TopologyKafkaSpout topologyKafkaSpout = new TopologyKafkaSpout();
	private DataBolt dataBolt = new DataBolt();
	private AlgorithmBolt algorithmBolt = new AlgorithmBolt();
	private GraphBolt graphBolt = new GraphBolt();

	/**
	 * This method creates a topology and submits to the cluster either to local
	 * or remote depending on the provided parameters
	 * 
	 * @param args
	 *            - Command Line arguments
	 * @throws AuthorizationException
	 * @throws InvalidTopologyException
	 * @throws AlreadyAliveException
	 * @throws Exception
	 * @AlreadAliveException @InvalidTopologyException @AuthorizationException
	 */
	protected void submitTopology(String[] args) throws AlreadyAliveException, InvalidTopologyException,
			AuthorizationException{

		if(ArrayUtils.isEmpty(args)){
			logger.error("Command line argument can not be null or empty");
			throw new IllegalArgumentException("Command line argument cannot be null or empty");
		}

		if(args.length != 4){
			logger.info("Argumements provided by user are:");
			Arrays.asList(args).forEach(logger::info);
			logger.error("Number of argument should be exactly four arguments in following order \n1. Topology Name \n2. RedisIP \n3. RedisPort \n4. RedisKey");
			throw new IllegalArgumentException(
					"\nNumber of argument should be exactly four arguments in following order \n1. Topology Name \n2. RedisIP \n3. RedisPort \n4. RedisKey");
		}
		Arrays.asList(args).forEach(logger::info);
		logger.info("Reading Topology properties from redis");
		Config config = loadTopologyProperties(args);
		logger.info("Configuration has been loaded from readis {}", config);

		KafkaSpout dataSpout = topologyKafkaSpout.buildKafkaSpout(buildPropertyMap(config),
				config.get(ExampleTopologyUtils.DATATOPIC).toString());
		KafkaSpout algorithmSpout = topologyKafkaSpout.buildKafkaSpout(buildPropertyMap(config),
				config.get(ExampleTopologyUtils.ALGORITHMTOPIC).toString());

		int spoutThreads = Integer.parseInt(config.get(ExampleTopologyUtils.SPOUTCOUNT).toString());
		int boltThreads = Integer.parseInt(config.get(ExampleTopologyUtils.BOLTCOUNT).toString());
		int numbWorkers = Integer.parseInt(config.get(ExampleTopologyUtils.WORKERTHREAD).toString());

		// setting up topology
		topologyBuilder.setSpout(KAFKADATASPOUTID, dataSpout, spoutThreads);
		topologyBuilder.setSpout(KAFKAALGORITHMSPOUTID, algorithmSpout, spoutThreads);
		topologyBuilder.setBolt(ALGORITHMBOLTID, algorithmBolt, boltThreads).shuffleGrouping(KAFKAALGORITHMSPOUTID);
		topologyBuilder.setBolt(DATABOLTID, dataBolt, boltThreads).shuffleGrouping(KAFKADATASPOUTID);
		InputDeclarer<BoltDeclarer> boltDeclarer = topologyBuilder.setBolt(GRAPHBOLTID, graphBolt, boltThreads)
				.shuffleGrouping(DATABOLTID, ExampleTopologyUtils.DATASTREAM);
		boltDeclarer.shuffleGrouping(ALGORITHMBOLTID, ExampleTopologyUtils.ALGORITHMSTREAM);

		/**
		 * submitting storm topology to cluster
		 */
		String topologyName = args[0];
		config.setNumWorkers(numbWorkers);
		StormSubmitter.submitTopology(topologyName, config, topologyBuilder.createTopology());
	}

	/**
	 * Reading all configurations of directory topology from Redis and loaded to
	 * to @Config storm object
	 * 
	 * @param args
	 *            -Arguments provided by user from command line
	 * @return @Config object
	 */
	public Config loadTopologyProperties(String[] args){
		// parsing the command line arguments
		String redisIp = args[1];
		int redisPort = Integer.parseInt(args[2]);
		String redisKey = args[3];

		// creating @Config object and loading that object from redis
		Config config = new Config();
		Redis redis = getRedis(redisIp, redisPort);

		// loading each topology value to the config
		ExampleTopologyUtils.getTopologyList().forEach(i -> {
			if(redis.exists(redisKey, i)){
				config.put(i, redis.getProperty(redisKey, i));
			}
		});

		// loading default config values for storm
		config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 2048);
		config.put(Config.TOPOLOGY_BACKPRESSURE_ENABLE, false);
		config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
		config.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
		return config;
	}

	public Redis getRedis(String redisIp, int redisPort){
		RedisClient redisClient = new RedisClient(redisIp, redisPort);
		return new Redis(redisClient);
	}

	/**
	 * Creating spout configuration for @Kafka @Zookeeper
	 * 
	 * @param config
	 *            -user provided configuration that is created with redis values
	 * @return @Map of @Zookeeper and zkRoot
	 */
	public Map<String, Object> buildPropertyMap(Config config){
		Map<String, Object> configProperties = new HashMap<>();
		configProperties.put(ExampleTopologyUtils.KAFKAZOOKEEPER, config.get(ExampleTopologyUtils.KAFKAZOOKEEPER));
		configProperties.put(ExampleTopologyUtils.KAFKAZKROOT, config.get(ExampleTopologyUtils.KAFKAZKROOT));
		return configProperties;
	}

	/**
	 * Main method to kick off the topology
	 * 
	 * @param args
	 *            - command line arguments
	 * @throws Exception
	 * @AlreadAliveException @InvalidTopologyException @AuthorizationException
	 */
	public static void main(String[] args) throws Exception{
		ExampleTopology topology = new ExampleTopology();
		topology.submitTopology(args);
	}
}
