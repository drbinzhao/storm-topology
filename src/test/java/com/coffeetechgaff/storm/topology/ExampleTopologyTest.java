package com.coffeetechgaff.storm.topology;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;

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
import com.coffeetechgaff.storm.redis.RedisClient;
import com.coffeetechgaff.storm.spout.TopologyKafkaSpout;
import com.coffeetechgaff.storm.topology.ExampleTopology;

/**
 * This test only test the main method of topology but this will not get covered
 * in sonar coverage
 * 
 * @author VivekSubedi
 *
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(ExampleTopology.class)
public class ExampleTopologyTest{

	private static final Logger logger = LoggerFactory.getLogger(ExampleTopology.class);

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
	public void testTopologyMainMethod() throws Exception{
		logger.info("Running testTopologyMainMethod...");
		ExampleTopology mockedDirectoryTopology = mock(ExampleTopology.class);
		PowerMockito.whenNew(ExampleTopology.class).withNoArguments().thenReturn(mockedDirectoryTopology);
		String[] args = new String[0];
		doNothing().when(mockedDirectoryTopology).submitTopology(args);

		// calling real method
		ExampleTopology.main(args);

		// verifying and asserting
		verify(mockedDirectoryTopology).submitTopology(args);
	}

	@Test
	public void testGetRedis() throws Exception{
		logger.info("Running testGetRedis...");
		String[] args = new String[]{"topology", "192.168.99.100", "9999", "directory"};
		String redisIp = args[1];
		int redisPort = Integer.parseInt(args[2]);

		RedisClient redisClient = mock(RedisClient.class);
		Redis redis = mock(Redis.class);

		PowerMockito.whenNew(RedisClient.class).withArguments(redisIp, redisPort).thenReturn(redisClient);
		PowerMockito.whenNew(Redis.class).withArguments(redisClient).thenReturn(redis);

		// calling real method
		Redis mockedRedis = topology.getRedis(redisIp, redisPort);

		// verifying and asserting
		assertEquals(redis, mockedRedis);
		verify(topology).getRedis(redisIp, redisPort);
	}
}
