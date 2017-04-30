package com.coffeetechgaff.storm.janusgraph;

import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Map;

import org.janusgraph.core.JanusGraph;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coffeetechgaff.storm.janusgraph.JanusGraphConnection;

/**
 * 
 * @author VivekSubedi
 *
 */
public class JanusGraphConnectionTest{
	private static final Logger logger = LoggerFactory.getLogger(JanusGraphConnectionTest.class);

	@InjectMocks
	private JanusGraphConnection graphConnection;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception{
		logger.info("=========== START UNIT TEST ===========");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception{
		logger.info("=========== END UNIT TEST ===========");
	}

	@Before
	public void setUp() throws Exception{
		MockitoAnnotations.initMocks(this);
	}

	@Test
	public void testLoadJanusGraph(){
		logger.info("Running testLoadJanusGraph...");
		JanusGraph graph = graphConnection.loadJanusGraph(buildPropertyMap());
		assertNotNull(graph);
		logger.info("Closing test graph...");
		graph.close();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testLoadJanusGraphWithEmptyMap(){
		logger.info("Running testLoadJanusGraphWithEmptyMap...");
		graphConnection.loadJanusGraph(new HashMap<>());
	}

	private Map<String, Object> buildPropertyMap(){
		Map<String, Object> configProperties = new HashMap<>();
		configProperties.put("storage.backend", "inmemory");
		return configProperties;
	}

}
