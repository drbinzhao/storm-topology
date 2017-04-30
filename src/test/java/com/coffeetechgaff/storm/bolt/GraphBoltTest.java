package com.coffeetechgaff.storm.bolt;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.janusgraph.core.JanusGraph;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coffeetechgaff.storm.algorithmnode.AlgorithmNode;
import com.coffeetechgaff.storm.bolt.GraphBolt;
import com.coffeetechgaff.storm.datanode.DataNode;
import com.coffeetechgaff.storm.exception.ExampleTopologyException;
import com.coffeetechgaff.storm.janusgraph.GraphVertex;
import com.coffeetechgaff.storm.janusgraph.JanusGraphConnection;
import com.coffeetechgaff.storm.utils.ExampleTopologyCommonTestUtils;
import com.coffeetechgaff.storm.utils.ExampleTopologyUtils;

/**
 * 
 * @author VivekSubedi
 *
 */
public class GraphBoltTest{

	private static final Logger logger = LoggerFactory.getLogger(GraphBoltTest.class);

	@InjectMocks
	@Spy
	private GraphBolt graphBolt;

	@Mock
	private GraphVertex<DataNode> datasourceGraph;

	@Mock
	private GraphVertex<AlgorithmNode> analyticGraph;

	@Mock
	private JanusGraphConnection graphConnection;

	@Mock
	private JanusGraph graph;

	@Mock
	private OutputCollector collector;

	private static String graphIndex = "vertices";

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

	@Test(expected = IllegalArgumentException.class)
	public void testPrepare() throws Exception{
		logger.info("Running testPrepare...");

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
		when(graphConnection.loadJanusGraph(buildPropertyMap())).thenReturn(graph);

		// calling method
		graphBolt.prepare(map, context, collector);

		// verifying the call
		verify(graphBolt, times(1)).prepare(map, context, collector);
	}

	@Test
	public void testExecuteDatasource() throws ExampleTopologyException{
		logger.info("Running testExecuteDatasource...");
		DataNode record = ExampleTopologyCommonTestUtils.createDataSourcenRecord();

		// mocking
		Tuple input = mock(Tuple.class);
		when(input.getSourceStreamId()).thenReturn(ExampleTopologyUtils.DATASOURCESTREAM);
		when(input.getValueByField("content")).thenReturn(ExampleTopologyCommonTestUtils.createDataSourcenRecord());
		when(datasourceGraph.makeVertex(record, graph, graphIndex)).thenReturn(12345678L);

		// call the method
		graphBolt.execute(input);

		// verify the call
		verify(graphBolt).execute(input);
		verify(collector).ack(input);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testExecuteDatasourceException() throws ExampleTopologyException{
		logger.info("Running testExecuteDatasourceException...");

		// mocking
		Tuple input = mock(Tuple.class);
		when(input.getSourceStreamId()).thenReturn(ExampleTopologyUtils.DATASOURCESTREAM);
		when(input.getValueByField("content")).thenReturn(ExampleTopologyCommonTestUtils.createDataSourcenRecord());
		when(datasourceGraph.makeVertex(ExampleTopologyCommonTestUtils.createDataSourcenRecord(), graph, graphIndex))
				.thenThrow(ExampleTopologyException.class);

		// call the method
		graphBolt.execute(input);

		// verify the call
		verify(graphBolt).execute(input);
		verify(collector).ack(input);
	}

	@Test
	public void testExecuteAnalytic() throws ExampleTopologyException{
		logger.info("Running testExecuteAnalytic...");

		// mocking
		Tuple input = mock(Tuple.class);
		when(input.getSourceStreamId()).thenReturn(ExampleTopologyUtils.ANALYTICSTREAM);
		when(input.getValueByField("content")).thenReturn(ExampleTopologyCommonTestUtils.getAnalyticDefinationObject());
		when(analyticGraph.makeVertex(ExampleTopologyCommonTestUtils.getAnalyticDefinationObject(), graph, graphIndex))
				.thenReturn(837259348L);

		// call the method
		graphBolt.execute(input);

		// verify the call
		verify(graphBolt).execute(input);
		verify(collector).ack(input);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testExecuteAnalyticException() throws ExampleTopologyException{
		logger.info("Running testExecuteAnalyticException...");

		// mocking
		Tuple input = mock(Tuple.class);
		when(input.getSourceStreamId()).thenReturn(ExampleTopologyUtils.ANALYTICSTREAM);
		when(input.getValueByField("content")).thenReturn(ExampleTopologyCommonTestUtils.getAnalyticDefinationObject());
		when(analyticGraph.makeVertex(ExampleTopologyCommonTestUtils.getAnalyticDefinationObject(), graph, graphIndex))
				.thenThrow(ExampleTopologyException.class);

		// call the method
		graphBolt.execute(input);

		// verify the call
		verify(graphBolt).execute(input);
		verify(collector).ack(input);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testExecuteForException(){
		logger.info("Running testExecuteForException...");

		// mocking
		Tuple input = mock(Tuple.class);
		when(input.getSourceStreamId()).thenReturn("Wrong Stream");

		// call the method
		graphBolt.execute(input);

		// verify the call
		verify(graphBolt).execute(input);
	}

	@Test
	public void testDeclareOutputFields(){
		logger.info("Running testDeclareOutputFields...");

		// mocking
		OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);

		// calling method
		graphBolt.declareOutputFields(declarer);

		// verifying the call
		verify(graphBolt).declareOutputFields(declarer);
	}

	@Test
	public void testCleanUp(){
		logger.info("Running testCleanUp...");
		graphBolt.cleanup();

		// verifying the call
		verify(graphBolt).cleanup();
	}

	private Map<String, Object> buildPropertyMap(){
		Map<String, Object> configProperties = new HashMap<>();
		configProperties.put("storage.backend", "inmemory");
		return configProperties;
	}
}
