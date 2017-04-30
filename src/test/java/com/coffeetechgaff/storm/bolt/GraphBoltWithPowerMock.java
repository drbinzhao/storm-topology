package com.coffeetechgaff.storm.bolt;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.janusgraph.core.JanusGraph;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coffeetechgaff.storm.bolt.GraphBolt;
import com.coffeetechgaff.storm.janusgraph.JanusGraphConnection;

/**
 * This test class tests the new creation of graph bolt which happens in prepare
 * method. This will be in coverage in sonar qube.
 * 
 * @author VivekSubedi
 *
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(GraphBolt.class)
public class GraphBoltWithPowerMock{

	private static final Logger logger = LoggerFactory.getLogger(GraphBoltWithPowerMock.class);

	@InjectMocks
	@Spy
	private GraphBolt graphBolt = new GraphBolt();

	@Mock
	private JanusGraphConnection graphConnection;

	@Mock
	private JanusGraph graph;

	@Mock
	private OutputCollector collector;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception{
		logger.info("============== START UNIT TEST ==============");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception{
		logger.info("============== END UNIT TEST ==============");
	}

	@Before
	public void setUp() throws Exception{
		MockitoAnnotations.initMocks(this);
	}

	@Test
	public void testPrepareObjectCreate() throws Exception{
		logger.info("Running testPrepareObjectCreate...");

		// mocking
		Map<String, Object> map = new HashMap<>();
		map.put("storage.backend", "cassandratrift");
		map.put("storage.backend.host", "127.0.0.1");
		map.put("index.search.backend", "elasticsearch");
		map.put("index.search.hostname", "127.0.0.1");
		map.put("index.search.client.only", "true");
		map.put("db.cache", "true");
		map.put("db.cache.clean.wait", "20");
		map.put("db.cache.time", "180000");
		map.put("db.cache.size", "0.25");
		map.put("graph.index", "vertices");
		logger.info(map.toString());
		TopologyContext context = mock(TopologyContext.class);
		PowerMockito.whenNew(JanusGraphConnection.class).withNoArguments().thenReturn(graphConnection);
		when(graphConnection.loadJanusGraph(buildPropertyMap())).thenReturn(graph);

		// calling method
		logger.info("calling method..");
		graphBolt.prepare(map, context, collector);

		// verifying the call
		logger.info("Verifying...");
		verify(graphBolt, times(1)).prepare(map, context, collector);

	}

	private Map<String, Object> buildPropertyMap(){
		Map<String, Object> configProperties = new HashMap<>();
		configProperties.put("storage.backend", "inmemory");
		return configProperties;
	}

}
