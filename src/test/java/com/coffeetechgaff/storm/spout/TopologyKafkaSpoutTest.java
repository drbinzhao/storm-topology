package com.coffeetechgaff.storm.spout;

import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.kafka.KafkaSpout;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coffeetechgaff.storm.spout.TopologyKafkaSpout;
import com.coffeetechgaff.storm.utils.ExampleTopologyUtils;

/**
 * 
 * @author VivekSubedi
 *
 */
public class TopologyKafkaSpoutTest{

	private static final Logger logger = LoggerFactory.getLogger(TopologyKafkaSpoutTest.class);

	@InjectMocks
	private TopologyKafkaSpout topologyKafkaSpout;

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
	public void testAnalyticBuildKafkaSpout(){
		logger.info("Running testAnalyticBuildKafkaSpout...");
		KafkaSpout spout = topologyKafkaSpout.buildKafkaSpout(buildPropertyMap(), "analytic");
		assertNotNull(spout);
	}

	@Test
	public void testDatasourceBuildKafkaSpout(){
		logger.info("Running testDatasourceBuildKafkaSpout...");
		KafkaSpout spout = topologyKafkaSpout.buildKafkaSpout(buildPropertyMap(), "datasource");
		assertNotNull(spout);
	}

	private Map<String, Object> buildPropertyMap(){
		Map<String, Object> configProperties = new HashMap<>();
		configProperties.put(ExampleTopologyUtils.KAFKAZOOKEEPER, "localhost");
		configProperties.put(ExampleTopologyUtils.KAFKAZKROOT, "/kafka");
		return configProperties;
	}

}
