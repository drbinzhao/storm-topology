package com.coffeetechgaff.storm.topology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coffeetechgaff.storm.bolt.AlgorithmBolt;
import com.coffeetechgaff.storm.bolt.DataBolt;
import com.coffeetechgaff.storm.bolt.GraphBolt;
import com.coffeetechgaff.storm.redis.Redis;
import com.coffeetechgaff.storm.spout.TopologyKafkaSpout;
import com.coffeetechgaff.storm.topology.ExampleTopology;
import com.coffeetechgaff.storm.utils.ExampleTopologyUtils;

/**
 * 
 * @author VivekSubedi]
 *
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(StormSubmitter.class)
public class ExampleTopologyPowerMockitoTest{

	private static final Logger logger = LoggerFactory.getLogger(ExampleTopologyPowerMockitoTest.class);

	@InjectMocks
	@Spy
	private ExampleTopology topology = new ExampleTopology();

	@Mock(answer = Answers.RETURNS_DEEP_STUBS)
	private TopologyBuilder topologyBuilder;

	@Mock
	private TopologyKafkaSpout directoryKafkaSpout;

	@Mock
	private DataBolt dataBolt;

	@Mock
	private AlgorithmBolt algorithmBolt;

	@Mock
	private GraphBolt graphBolt;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception{
		logger.info("============ START UNIT TEST ==============");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception{
		logger.info("============ END UNIT TEST ==============");
	}

	@Before
	public void setUp() throws Exception{
		MockitoAnnotations.initMocks(this);
	}

	@Test
	public void testSubmitTopologyRemotely() throws Exception{
		logger.info("Running testSubmitTopologyRemotely...");
		ExampleTopology spyTopology = spy(ExampleTopology.class);
		String[] args = new String[]{"topology", "192.168.99.100", "9999", "directory"};

		// mocking
		PowerMockito.mockStatic(StormSubmitter.class);
		PowerMockito.doNothing().when(StormSubmitter.class);
		StormSubmitter.submitTopology(any(String.class), any(Config.class), any(StormTopology.class));
		doReturn(getConfig()).when(spyTopology).loadTopologyProperties(args);

		// calling real method
		spyTopology.submitTopology(args);

		// verifying the call
		verify(spyTopology).submitTopology(args);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSubmitTopologyLocally() throws Exception{
		logger.info("Running testSubmitTopologyLocally...");

		String[] args = new String[0];

		// calling real method
		topology.submitTopology(args);

		// verifying and asserting
		verify(topology).submitTopology(args);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSubmitTopologyLocallyWithNullArgument() throws Exception{
		logger.info("Running testSubmitTopologyLocallyWithNullArgument...");

		String[] args = new String[]{"topology", "192.168.99.100"};

		// calling real method
		topology.submitTopology(args);

		// verifying and asserting
		verify(topology).submitTopology(args);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testTopologyMainMethod() throws Exception{
		logger.info("Running testTopologyMainMethod...");
		String[] args = new String[0];

		// calling real method and we are expecting it throw
		// illegalArgumentException because Arg is empty
		ExampleTopology.main(args);
	}

	@Test
	public void testLoadTopologyProperties(){
		logger.info("Running testLoadTopologyProperties...");

		// loading data
		String[] args = new String[]{"topology", "192.168.99.100", "9999", "directory"};
		String redisIp = args[1];
		int redisPort = Integer.parseInt(args[2]);
		String redisKey = args[3];
		String datasourceTopic = "datasource.topic";
		String analyticTopic = "analytic.topic";

		// mocking
		ExampleTopology spyTopology = spy(ExampleTopology.class);
		Redis redis = mock(Redis.class);
		doReturn(redis).when(spyTopology).getRedis(redisIp, redisPort);
		when(redis.exists(redisKey, datasourceTopic)).thenReturn(true);
		when(redis.exists(redisKey, analyticTopic)).thenReturn(true);
		when(redis.getProperty(redisKey, datasourceTopic)).thenReturn("datasources");
		when(redis.getProperty(redisKey, analyticTopic)).thenReturn("avroNode");

		// calling real method
		Config config = spyTopology.loadTopologyProperties(args);

		// veryfing the call
		logger.info(config.toString());
		assertNotNull(config);
		assertEquals(6, config.entrySet().size());
		assertEquals("datasources", config.get(datasourceTopic));
		assertEquals("avroNode", config.get(analyticTopic));
	}

	private Config getConfig(){
		Config config = new Config();
		config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 2048);
		config.put(Config.TOPOLOGY_BACKPRESSURE_ENABLE, false);
		config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
		config.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
		config.put(ExampleTopologyUtils.ANALYTICTOPIC, "avroNode");
		config.put(ExampleTopologyUtils.DATASOURCETOPIC, "datasources");
		config.put(ExampleTopologyUtils.SPOUTCOUNT, "1");
		config.put(ExampleTopologyUtils.BOLTCOUNT, "1");
		config.put(ExampleTopologyUtils.WORKERTHREAD, "1");
		config.put(ExampleTopologyUtils.KAFKAZOOKEEPER, "localhost");
		config.put(ExampleTopologyUtils.KAFKAZKROOT, "/kafka");
		return config;
	}

}
