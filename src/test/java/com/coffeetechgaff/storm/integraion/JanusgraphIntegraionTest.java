package com.coffeetechgaff.storm.integraion;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coffeetechgaff.storm.algorithmnode.AlgorithmNode;
import com.coffeetechgaff.storm.datanode.DataNode;
import com.coffeetechgaff.storm.datanode.Operation;
import com.coffeetechgaff.storm.exception.ExampleTopologyException;
import com.coffeetechgaff.storm.janusgraph.AlgorithmVertexCreator;
import com.coffeetechgaff.storm.janusgraph.CommonVertexCode;
import com.coffeetechgaff.storm.janusgraph.DataVertexCreator;
import com.coffeetechgaff.storm.janusgraph.GraphVertex;
import com.coffeetechgaff.storm.janusgraph.JanusGraphConnection;
import com.coffeetechgaff.storm.utils.ExampleTopologyCommonTestUtils;

/**
 * 
 * @author VivekSubedi
 *
 */

@Ignore
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class JanusgraphIntegraionTest{
	private static Logger logger = LoggerFactory.getLogger(JanusgraphIntegraionTest.class);

	private static GraphVertex<DataNode> dataSourceGraph;
	private static GraphVertex<AlgorithmNode> analyticsGraph;
	private static CommonVertexCode commonVertexCode;
	private static JanusGraphConnection graphConnection;
	private static JanusGraph graph;
	private static String graphIndex = "search";

	// datasource vertex ID
	private static long vertexId = 0l;
	private static long vertexId2 = 0l;
	private static long vertexId3 = 0l;
	private static long vertexId4 = 0l;
	private static long vertexId5 = 0l;
	private static long vertexId6 = 0l;

	// analytics vertex ID
	private static long analyticsVertexId = 0l;
	private static long analyticsVertexId2 = 0l;
	private static long analyticsVertexId3 = 0l;
	private static long analyticsVertexId4 = 0l;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception{
		dataSourceGraph = new DataVertexCreator();
		commonVertexCode = new CommonVertexCode();
		graphConnection = new JanusGraphConnection();
		analyticsGraph = new AlgorithmVertexCreator();

		Map<String, Object> configMap = new HashMap<>();
		configMap.put("storage.backend", "cassandrathrift");
		configMap.put("storage.hostname", "192.168.99.100");
		configMap.put("cache.db-cache", true);
		configMap.put("index.search.backend", "elasticsearch");
		configMap.put("index.search.hostname", "192.168.99.100");
		configMap.put("index.search.elasticsearch.client-only", true);
		graph = graphConnection.loadJanusGraph(configMap);
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception{
		dataSourceGraph = null;
		graphConnection = null;
		commonVertexCode.closeGraph(graph);
		commonVertexCode = null;
	}

	@Test
	public void createVertexes() throws ExampleTopologyException{
		logger.info("Running createVertexes...");
		// create first analytics vertex
		analyticsVertexId = analyticsGraph.makeVertex(createAnalyticsRecord1(), graph, graphIndex);
		assertNotEquals(0l, analyticsVertexId);
		logger.info("Analytics 1 [{}]", analyticsVertexId);

		// create second analytics vertex
		analyticsVertexId2 = analyticsGraph.makeVertex(createAnalyticsRecord2(), graph, graphIndex);
		assertNotEquals(0l, analyticsVertexId2);
		logger.info("Analytics 2 [{}]", analyticsVertexId2);

		// create second analytics vertex
		analyticsVertexId3 = analyticsGraph.makeVertex(createAnalyticsRecord3(), graph, graphIndex);
		assertNotEquals(0l, analyticsVertexId3);
		logger.info("Analytics 3 [{}]", analyticsVertexId3);

		analyticsVertexId4 = analyticsGraph.makeVertex(createAnalyticsRecord4(), graph, graphIndex);
		assertNotEquals(0l, analyticsVertexId4);
		logger.info("Analytics 3 [{}]", analyticsVertexId4);

		// first
		vertexId = dataSourceGraph.makeVertex(createDataSourceRecord1(), graph, graphIndex);
		assertNotEquals(0l, vertexId);
		logger.info("Datasource 1 [{}]", vertexId);

		// second
		vertexId2 = dataSourceGraph.makeVertex(createDataSourceRecord2(), graph, graphIndex);
		logger.info("Datasource 2 [{}]", vertexId2);
		assertNotEquals(0l, vertexId2);

		// third
		vertexId3 = dataSourceGraph.makeVertex(createDataSourceRecord3(), graph, graphIndex);
		logger.info("Datasource 3 [{}]", vertexId3);
		assertNotEquals(0l, vertexId3);

		// fourth
		vertexId4 = dataSourceGraph.makeVertex(createDataSourceRecord4(), graph, graphIndex);
		logger.info("Datasource 4 [{}]", vertexId4);
		assertNotEquals(0l, vertexId4);

		DataNode ds = createDataSourceRecord4();
		ds.setDataTypes(null);
		vertexId5 = dataSourceGraph.makeVertex(ds, graph, graphIndex);
		assertNotEquals(0l, vertexId5);
		logger.info("Datasource 5 [{}]", vertexId5);

		ds.setDataTypes(new ArrayList<>());
		vertexId6 = dataSourceGraph.makeVertex(ds, graph, graphIndex);
		assertNotEquals(0l, vertexId6);
		logger.info("Datasource 6 [{}]", vertexId6);
	}

	@Test
	public void readVertexes(){
		logger.info("Running readVertexes...");
		List<Vertex> readVertexId1 = commonVertexCode.getVertexId(vertexId, graph);
		assertEquals(vertexId, readVertexId1.get(0).id());

		List<Vertex> readVertexId2 = commonVertexCode.getVertexId(vertexId2, graph);
		assertEquals(vertexId2, readVertexId2.get(0).id());

		List<Vertex> readVertexId3 = commonVertexCode.getVertexId(vertexId3, graph);
		assertEquals(vertexId3, readVertexId3.get(0).id());

		List<Vertex> readVertexId4 = commonVertexCode.getVertexId(vertexId4, graph);
		assertEquals(vertexId4, readVertexId4.get(0).id());

		List<Vertex> readVertexId5 = commonVertexCode.getVertexId(vertexId5, graph);
		assertEquals(vertexId5, readVertexId5.get(0).id());

		List<Vertex> readVertexId6 = commonVertexCode.getVertexId(vertexId6, graph);
		assertEquals(vertexId6, readVertexId6.get(0).id());

		// analytics
		List<Vertex> readVertexAnalyticsId1 = commonVertexCode.getVertexId(analyticsVertexId, graph);
		assertEquals(analyticsVertexId, readVertexAnalyticsId1.get(0).id());

		List<Vertex> readVertexAnalyticsId2 = commonVertexCode.getVertexId(analyticsVertexId2, graph);
		assertEquals(analyticsVertexId2, readVertexAnalyticsId2.get(0).id());

		List<Vertex> readVertexAnalyticsId3 = commonVertexCode.getVertexId(analyticsVertexId3, graph);
		assertEquals(analyticsVertexId3, readVertexAnalyticsId3.get(0).id());
	}

	@Test
	public void testDefineRelationship(){
		logger.info("Running testDefineRelationship...");
		// checking relationship
		List<Vertex> outGoingVertexId = commonVertexCode.getOutGoingToVertices(vertexId, graph);
		assertEquals(4, outGoingVertexId.size());

		List<Vertex> outGoingVertexId2 = commonVertexCode.getOutGoingToVertices(vertexId2, graph);
		assertEquals(1, outGoingVertexId2.size());

		List<Vertex> outGoingVertexId3 = commonVertexCode.getOutGoingToVertices(vertexId3, graph);
		assertEquals(2, outGoingVertexId3.size());

		List<Vertex> outIngoingVertexId = commonVertexCode.getOutGoingToVertices(vertexId4, graph);
		assertEquals(1, outIngoingVertexId.size());

		List<Vertex> outGoingVertexId5 = commonVertexCode.getOutGoingToVertices(vertexId5, graph);
		assertEquals(0, outGoingVertexId5.size());

		List<Vertex> outGoingVertexId6 = commonVertexCode.getOutGoingToVertices(vertexId6, graph);
		assertEquals(0, outGoingVertexId6.size());

		List<Vertex> outGoingAnalyticVertexIds = commonVertexCode.getOutGoingToVertices(analyticsVertexId, graph);
		assertEquals(2, outGoingAnalyticVertexIds.size());
		List<Vertex> outGoingAnalyticVertexIds2 = commonVertexCode.getOutGoingToVertices(analyticsVertexId2, graph);
		assertEquals(2, outGoingAnalyticVertexIds2.size());
		List<Vertex> outGoingAnalyticVertexIds3 = commonVertexCode.getOutGoingToVertices(analyticsVertexId3, graph);
		assertEquals(2, outGoingAnalyticVertexIds3.size());
		List<Vertex> outGoingAnalyticVertexIds4 = commonVertexCode.getOutGoingToVertices(analyticsVertexId4, graph);
		assertEquals(2, outGoingAnalyticVertexIds4.size());
	}

	@Test
	public void updateVertices() throws ExampleTopologyException{
		logger.info("Running updateVertices...");
		// first
		DataNode ds1 = createDataSourceRecord1();
		ds1.setOperation(Operation.UPDATE);
		ds1.setDescription("This is test1 update");
		ds1.setDataTypes(Arrays.asList(new String[]{"com.coffeetechgaff.storm.algorithmnode.example"}));
		long updatedVertexId1 = dataSourceGraph.makeVertex(ds1, graph, graphIndex);
		logger.info("Dataosurce 1 [{}]", updatedVertexId1);
		assertEquals(vertexId, updatedVertexId1);
		List<Vertex> v1 = commonVertexCode.getVertexId(updatedVertexId1, graph);
		assertEquals(1, v1.size());
		assertEquals("This is test1 update", v1.get(0).value("description"));

		// second
		DataNode ds2 = createDataSourceRecord2();
		ds2.setOperation(Operation.UPDATE);
		ds2.setDescription("This is test2 update");
		long updatedVertexId2 = dataSourceGraph.makeVertex(ds2, graph, graphIndex);
		logger.info("Dataosurce 2 [{}]" + updatedVertexId2);
		assertEquals(vertexId2, updatedVertexId2);
		List<Vertex> v2 = commonVertexCode.getVertexId(updatedVertexId2, graph);
		assertEquals(1, v2.size());
		assertEquals("This is test2 update", v2.get(0).value("description"));

		// third
		DataNode ds3 = createDataSourceRecord3();
		ds3.setDataTypes(null);
		ds3.setOperation(Operation.UPDATE);
		ds3.setDescription("This is test3 update with null datatypes");
		long updatedVertexId3 = dataSourceGraph.makeVertex(ds3, graph, graphIndex);
		logger.info("Dataosurce 3 [{}]", updatedVertexId3);
		assertEquals(vertexId3, updatedVertexId3);
		List<Vertex> v3 = commonVertexCode.getVertexId(updatedVertexId3, graph);
		assertEquals(1, v3.size());
		assertEquals("This is test3 update with null datatypes", v3.get(0).value("description"));

		// fourth
		DataNode ds4 = createDataSourceRecord3();
		ds4.setDataTypes(new ArrayList<>());
		ds4.setOperation(Operation.UPDATE);
		ds4.setDescription("This is test3 update without dataTypes");
		long updatedVertexId4 = dataSourceGraph.makeVertex(ds4, graph, graphIndex);
		logger.info("Dataosurce 4 [{}]", updatedVertexId4);
		assertEquals(vertexId3, updatedVertexId4);
		List<Vertex> v4 = commonVertexCode.getVertexId(updatedVertexId4, graph);
		assertEquals(1, v4.size());
		assertEquals("This is test3 update without dataTypes", v4.get(0).value("description"));
	}

	@Test
	public void testOperationError() throws ExampleTopologyException{
		DataNode ar = createDataSourceRecord1();
		ar.setOperation(Operation.valueOf("WRONG"));
		long wrongVertexId = dataSourceGraph.makeVertex(ar, graph, graphIndex);
		assertEquals(0l, wrongVertexId);
	}

	@Test
	public void updateVerticesWithDeleteOparation() throws ExampleTopologyException{
		logger.info("Running updateVerticesWithDeleteOparation...");
		// first
		DataNode ds1 = createDataSourceRecord1();
		ds1.setOperation(Operation.DELETE);
		long deletedVertexId1 = dataSourceGraph.makeVertex(ds1, graph, graphIndex);
		assertEquals(vertexId, deletedVertexId1);

		// second
		DataNode ds2 = createDataSourceRecord2();
		ds2.setOperation(Operation.DELETE);
		long deletedVertexId2 = dataSourceGraph.makeVertex(ds2, graph, graphIndex);
		assertEquals(vertexId2, deletedVertexId2);

		// third
		DataNode ds3 = createDataSourceRecord3();
		ds3.setOperation(Operation.DELETE);
		long deletedVertexId3 = dataSourceGraph.makeVertex(ds3, graph, graphIndex);
		assertEquals(vertexId3, deletedVertexId3);

		// checking to make sure the vertices has been deleted
		List<Vertex> v1 = commonVertexCode.getVertexId(deletedVertexId1, graph);
		assertTrue(v1.isEmpty());
		List<Vertex> v2 = commonVertexCode.getVertexId(deletedVertexId2, graph);
		assertTrue(v2.isEmpty());
		List<Vertex> v3 = commonVertexCode.getVertexId(deletedVertexId2, graph);
		assertTrue(v3.isEmpty());
	}

	private DataNode createDataSourceRecord1(){
		DataNode node = new DataNode();
		node.setClassification("unclassified");
		node.setName("Geometry");
		node.setDataTypes(Arrays.asList(new String[]{"geometry", "polygon"}));
		node.setDescription("This is for test1");
		node.setMaturity("gold");
		node.setId("12345");
		node.setOperation(com.coffeetechgaff.storm.datanode.Operation.CREATE);
		return node;
	}

	private DataNode createDataSourceRecord2(){
		DataNode node = new DataNode();
		node.setClassification("unclassified");
		node.setName("Triangle");
		node.setDataTypes(Arrays.asList(new String[]{"triangle", "square"}));
		node.setDescription("This is for test2");
		node.setMaturity("gold");
		node.setId("123456");
		node.setOperation(com.coffeetechgaff.storm.datanode.Operation.CREATE);
		return node;
	}

	private DataNode createDataSourceRecord3(){
		DataNode node = new DataNode();
		node.setClassification("unclassified");
		node.setName("Rectangle");
		node.setDataTypes(Arrays.asList(new String[]{"rectangle", "circle"}));
		node.setDescription("This is for test3");
		node.setMaturity("iron");
		node.setId("234567");
		node.setOperation(com.coffeetechgaff.storm.datanode.Operation.CREATE);
		return node;
	}

	private DataNode createDataSourceRecord4(){
		DataNode node = new DataNode();
		node.setClassification("unclassified");
		node.setName("Hexagon");
		node.setDataTypes(Arrays.asList(new String[]{"hexagon", "texagon"}));
		node.setDescription("This is for test4");
		node.setMaturity("bust");
		node.setId("345678");
		node.setOperation(com.coffeetechgaff.storm.datanode.Operation.CREATE);
		return node;
	}

	private AlgorithmNode createAnalyticsRecord1(){
		return ExampleTopologyCommonTestUtils.getAnalyticDefinationObject();
	}

	private AlgorithmNode createAnalyticsRecord4(){
		return ExampleTopologyCommonTestUtils.getAnalyticDefinationObject2();
	}

	private AlgorithmNode createAnalyticsRecord2(){
		return ExampleTopologyCommonTestUtils.getAnalyticDefinationObject3();
	}

	private AlgorithmNode createAnalyticsRecord3(){
		return ExampleTopologyCommonTestUtils.getAnalyticDefinationObject4();
	}

}
