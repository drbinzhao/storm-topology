package com.coffeetechgaff.storm.janusgraph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.json.JSONArray;
import org.json.JSONException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coffeetechgaff.storm.algorithmnode.AlgorithmNode;
import com.coffeetechgaff.storm.algorithmnode.NodeConfig;
import com.coffeetechgaff.storm.algorithmnode.Operation;
import com.coffeetechgaff.storm.datanode.DataNode;
import com.coffeetechgaff.storm.exception.ExampleTopologyException;
import com.coffeetechgaff.storm.exception.ObjectIsNullException;
import com.coffeetechgaff.storm.exception.ValidatedMapEmptyException;
import com.coffeetechgaff.storm.exception.VertexNotFoundException;
import com.coffeetechgaff.storm.janusgraph.AlgorithmVertexCreator;
import com.coffeetechgaff.storm.janusgraph.CommonVertexCode;
import com.coffeetechgaff.storm.janusgraph.DataVertexCreator;
import com.coffeetechgaff.storm.janusgraph.GraphVertex;
import com.coffeetechgaff.storm.janusgraph.JanusGraphConnection;
import com.coffeetechgaff.storm.utils.ExampleTopologyCommonTestUtils;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AlgorithmNodeGraphTest{

	private static Logger logger = LoggerFactory.getLogger(AlgorithmNodeGraphTest.class);

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

	// analytics vertex ID
	private static long analyticsVertexId = 0l;
	private static long analyticsVertexId2 = 0l;
	private static long analyticsVertexId3 = 0l;
	private static long analyticsVertexId4 = 0l;
	private static long analyticsVertexId5 = 0l;
	private static long analyticsVertexId6 = 0l;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception{
		dataSourceGraph = new DataVertexCreator();
		commonVertexCode = new CommonVertexCode();
		graphConnection = new JanusGraphConnection();
		analyticsGraph = new AlgorithmVertexCreator();

		Map<String, Object> map = new HashMap<>();
		map.put("storage.backend", "inmemory");

		// loading janus graph
		graph = graphConnection.loadJanusGraph(map);
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception{
		dataSourceGraph = null;
		graphConnection = null;
		commonVertexCode.closeGraph(graph);
		commonVertexCode = null;
	}

	@Test(expected = ObjectIsNullException.class)
	public void testNullVertexObjects() throws ExampleTopologyException{
		logger.info("Running testNullVertexObjects...");
		analyticsGraph.makeVertex(null, graph, graphIndex);
	}

	@Test(expected = ObjectIsNullException.class)
	public void testNullGraphObjects() throws ExampleTopologyException{
		logger.info("Running testNullGraphObjects...");
		analyticsGraph.makeVertex(createAnalyticsRecord1(), null, graphIndex);
	}

	@Test(expected = ObjectIsNullException.class)
	public void testNullIndexObjects() throws ExampleTopologyException{
		logger.info("Running testNullIndexObjects...");
		analyticsGraph.makeVertex(createAnalyticsRecord1(), graph, null);
	}

	@Test
	public void testCreateVertexes() throws ExampleTopologyException{
		logger.info("Running testCreateVertexes...");

		/**
		 * Datasource
		 */
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

		/**
		 * Analytics
		 */
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
		logger.info("Analytics 4 [{}]", analyticsVertexId4);

		AlgorithmNode record = createAnalyticsRecord4();
		record.setUid("987492874957");
		record.setInput(null);
		analyticsVertexId5 = analyticsGraph.makeVertex(record, graph, graphIndex);
		assertNotEquals(0l, analyticsVertexId5);
		logger.info("Analytics 5 [{}]", analyticsVertexId5);

		AlgorithmNode record1 = createAnalyticsRecord4();
		record1.setUid("0983275498273");
		record1.getInput().get(0).setType(null);
		record1.getInput().get(1).setType(null);
		analyticsVertexId6 = analyticsGraph.makeVertex(record1, graph, graphIndex);
		assertNotEquals(0l, analyticsVertexId6);
		logger.info("Analytics 6 [{}]", analyticsVertexId6);
	}

	@Test
	public void testReadVertexes(){
		logger.info("Running testReadVertexes...");
		List<Vertex> readVertexId1 = commonVertexCode.getVertexId(vertexId, graph);
		assertEquals(vertexId, readVertexId1.get(0).id());

		List<Vertex> readVertexId2 = commonVertexCode.getVertexId(vertexId2, graph);
		assertEquals(vertexId2, readVertexId2.get(0).id());

		List<Vertex> readVertexId3 = commonVertexCode.getVertexId(vertexId3, graph);
		assertEquals(vertexId3, readVertexId3.get(0).id());

		List<Vertex> readVertexId4 = commonVertexCode.getVertexId(vertexId4, graph);
		assertEquals(vertexId4, readVertexId4.get(0).id());

		// analytics
		List<Vertex> readVertexAnalyticsId1 = commonVertexCode.getVertexId(analyticsVertexId, graph);
		assertEquals(analyticsVertexId, readVertexAnalyticsId1.get(0).id());

		List<Vertex> readVertexAnalyticsId2 = commonVertexCode.getVertexId(analyticsVertexId2, graph);
		assertEquals(analyticsVertexId2, readVertexAnalyticsId2.get(0).id());

		List<Vertex> readVertexAnalyticsId3 = commonVertexCode.getVertexId(analyticsVertexId3, graph);
		assertEquals(analyticsVertexId3, readVertexAnalyticsId3.get(0).id());

		List<Vertex> readVertexAnalyticsId4 = commonVertexCode.getVertexId(analyticsVertexId4, graph);
		assertEquals(analyticsVertexId4, readVertexAnalyticsId4.get(0).id());

		List<Vertex> readVertexAnalyticsId5 = commonVertexCode.getVertexId(analyticsVertexId5, graph);
		assertEquals(analyticsVertexId5, readVertexAnalyticsId5.get(0).id());

	}

	@Test
	public void testDefineRelationship(){
		logger.info("Running testDefineRelationship...");
		List<Vertex> outGoingVertexId = commonVertexCode.getOutGoingToVertices(analyticsVertexId, graph);
		outGoingVertexId.forEach(vertex -> {
			logger.info("Vertex id [{}] and Vertex name [{}] ", vertex.id(), vertex.value("name"));
		});
		assertEquals(2, outGoingVertexId.size());

		List<Vertex> outGoingVertexId2 = commonVertexCode.getOutGoingToVertices(analyticsVertexId2, graph);
		outGoingVertexId2.forEach(vertex -> {
			logger.info("Vertex id [{}] and Vertex name [{}] ", vertex.id(), vertex.value("name"));
		});
		assertEquals(2, outGoingVertexId2.size());

		List<Vertex> outIngoingVertexId = commonVertexCode.getIncomingVertices(analyticsVertexId3, graph);
		outIngoingVertexId.forEach(vertex -> {
			logger.info("Vertex id [{}] and Vertex name [{}] ", vertex.id(), vertex.value("name"));
		});
		assertEquals(2, outIngoingVertexId.size());

		List<Vertex> outGoingVertexId5 = commonVertexCode.getOutGoingToVertices(analyticsVertexId5, graph);
		assertEquals(0, outGoingVertexId5.size());

		List<Vertex> incomingVertexId5 = commonVertexCode.getIncomingVertices(analyticsVertexId5, graph);
		assertEquals(0, incomingVertexId5.size());

		List<Vertex> outGoingVertexId6 = commonVertexCode.getOutGoingToVertices(analyticsVertexId6, graph);
		assertEquals(0, outGoingVertexId6.size());
	}

	@Test
	public void testUpdateVertices() throws NoSuchElementException, JSONException, ExampleTopologyException{
		logger.info("Running updateVertices...");

		AlgorithmNode an1 = createAnalyticsRecord1();
		an1.setOperation(Operation.UPDATE);
		an1.setDescription("This test1 update for analytic record");
		NodeConfig config = an1.getInput().get(0);
		config.setType("com.coffeetechgaff.storm.algorithmnode.example");
		List<NodeConfig> configList = new ArrayList<>();
		configList.add(config);
		an1.setInput(configList);
		long updatedVertexId1 = analyticsGraph.makeVertex(an1, graph, graphIndex);
		assertEquals(analyticsVertexId, updatedVertexId1);
		List<Vertex> v1 = commonVertexCode.getVertexId(updatedVertexId1, graph);
		assertEquals(1, v1.size());
		assertEquals("This test1 update for analytic record", v1.get(0).value("description"));
		long v1id = (long) v1.get(0).id();
		List<Vertex> outGoingVertex = commonVertexCode.getOutGoingToVertices(v1id, graph);
		assertEquals(0, outGoingVertex.size());
		List<Vertex> incomingVeretx = commonVertexCode.getIncomingVertices(v1id, graph);
		assertEquals(0, incomingVeretx.size());

		AlgorithmNode an2 = createAnalyticsRecord2();
		an2.setOperation(Operation.UPDATE);
		an2.setDescription("This is graph database");
		long updatedVertexId2 = analyticsGraph.makeVertex(an2, graph, graphIndex);
		assertEquals(analyticsVertexId2, updatedVertexId2);
		List<Vertex> v2 = commonVertexCode.getVertexId(updatedVertexId2, graph);
		assertEquals(1, v2.size());
		assertEquals("This is graph database", v2.get(0).value("description"));

		AlgorithmNode an3 = createAnalyticsRecord3();
		List<NodeConfig> inputConfig = an3.getInput();
		an3.setInput(null);
		an3.setOperation(Operation.UPDATE);
		an3.setDescription("This is graph database");
		long updatedVertexId3 = analyticsGraph.makeVertex(an3, graph, graphIndex);
		assertEquals(analyticsVertexId3, updatedVertexId3);
		List<Vertex> v3 = commonVertexCode.getVertexId(updatedVertexId3, graph);
		assertEquals(1, v3.size());
		assertEquals("This is graph database", v3.get(0).value("description"));
		JSONArray jsonArray1 = new JSONArray(inputConfig.toString());
		JSONArray jsonArray2 = new JSONArray(v3.get(0).value("input").toString());
		assertEquals(jsonArray1.toString(), jsonArray2.toString());
	}

	@Test
	public void testUpdateVerticesWithDeleteOperation() throws ExampleTopologyException{
		logger.info("Running updateVerticesWithDeleteOperation...");

		AlgorithmNode an1 = createAnalyticsRecord1();
		an1.setOperation(Operation.DELETE);
		long deletedVertexId1 = analyticsGraph.makeVertex(an1, graph, graphIndex);
		assertEquals(analyticsVertexId, deletedVertexId1);

		AlgorithmNode an2 = createAnalyticsRecord2();
		an2.setOperation(Operation.DELETE);
		long deletedVertexId2 = analyticsGraph.makeVertex(an2, graph, graphIndex);
		assertEquals(analyticsVertexId2, deletedVertexId2);

		// checking to make sure the vertices has been deleted
		List<Vertex> v1 = commonVertexCode.getVertexId(deletedVertexId1, graph);
		assertTrue(v1.isEmpty());
		List<Vertex> v2 = commonVertexCode.getVertexId(deletedVertexId2, graph);
		assertTrue(v2.isEmpty());
	}

	@Test
	public void testOperationError() throws ExampleTopologyException{
		logger.info("Running testOperationError...");
		AlgorithmNode ar = createAnalyticsRecord1();
		ar.setOperation(Operation.valueOf("WRONG"));
		long wrongVertexId = analyticsGraph.makeVertex(ar, graph, graphIndex);
		assertEquals(0l, wrongVertexId);
	}

	@Test(expected = VertexNotFoundException.class)
	public void updateVerticesThatDoesntExists() throws ExampleTopologyException{
		logger.info("Running updateVerticesThatDoesntExists...");
		AlgorithmNode ds5 = createAnalyticsRecord1();
		ds5.setUid("4958734927");
		ds5.setOperation(Operation.UPDATE);
		ds5.setDescription("This is test3 update without dataTypes");
		analyticsGraph.makeVertex(ds5, graph, graphIndex);
	}

	@Test(expected = ValidatedMapEmptyException.class)
	public void testValidateMapUpdateIsEmpty() throws ExampleTopologyException{
		logger.info("Running testValidateMapUpdateIsEmpty...");
		AlgorithmNode anr = new AlgorithmNode();
		anr.setAuthor("");
		anr.setEmail("");
		anr.setOperation(Operation.UPDATE);
		analyticsGraph.makeVertex(anr, graph, graphIndex);
	}

	@Test(expected = ObjectIsNullException.class)
	public void testCreateObjectisNull() throws ExampleTopologyException{
		logger.info("Running testCreateObjectisNull...");
		analyticsGraph.createVertex(null);
	}

	@Test(expected = ValidatedMapEmptyException.class)
	public void testValidateMapCreateIsEmpty() throws ExampleTopologyException{
		logger.info("Running testValidateMapCreateIsEmpty...");
		AlgorithmNode anr = new AlgorithmNode();
		anr.setAuthor("");
		anr.setEmail("");
		anr.setOperation(Operation.CREATE);
		analyticsGraph.makeVertex(anr, graph, graphIndex);
	}

	@Test
	public void testGraphClose(){
		commonVertexCode.closeGraph(null);
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
