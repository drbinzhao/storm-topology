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

	private static GraphVertex<DataNode> dataGraph;
	private static GraphVertex<AlgorithmNode> algorithmGraph;
	private static CommonVertexCode commonVertexCode;
	private static JanusGraphConnection graphConnection;
	private static JanusGraph graph;
	private static String graphIndex = "search";

	// data vertex ID
	private static long vertexId = 0l;
	private static long vertexId2 = 0l;
	private static long vertexId3 = 0l;
	private static long vertexId4 = 0l;

	// algorithm vertex ID
	private static long algorithmVertexId = 0l;
	private static long algorithmVertexId2 = 0l;
	private static long algorithmVertexId3 = 0l;
	private static long algorithmVertexId4 = 0l;
	private static long algorithmVertexId5 = 0l;
	private static long algorithmVertexId6 = 0l;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception{
		dataGraph = new DataVertexCreator();
		commonVertexCode = new CommonVertexCode();
		graphConnection = new JanusGraphConnection();
		algorithmGraph = new AlgorithmVertexCreator();

		Map<String, Object> map = new HashMap<>();
		map.put("storage.backend", "inmemory");

		// loading janus graph
		graph = graphConnection.loadJanusGraph(map);
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception{
		dataGraph = null;
		graphConnection = null;
		commonVertexCode.closeGraph(graph);
		commonVertexCode = null;
	}

	@Test(expected = ObjectIsNullException.class)
	public void testNullVertexObjects() throws ExampleTopologyException{
		logger.info("Running testNullVertexObjects...");
		algorithmGraph.makeVertex(null, graph, graphIndex);
	}

	@Test(expected = ObjectIsNullException.class)
	public void testNullGraphObjects() throws ExampleTopologyException{
		logger.info("Running testNullGraphObjects...");
		algorithmGraph.makeVertex(createAlgorithmRecord1(), null, graphIndex);
	}

	@Test(expected = ObjectIsNullException.class)
	public void testNullIndexObjects() throws ExampleTopologyException{
		logger.info("Running testNullIndexObjects...");
		algorithmGraph.makeVertex(createAlgorithmRecord1(), graph, null);
	}

	@Test
	public void testCreateVertexes() throws ExampleTopologyException{
		logger.info("Running testCreateVertexes...");

		/**
		 * Data
		 */
		// first
		vertexId = dataGraph.makeVertex(createDataRecord1(), graph, graphIndex);
		assertNotEquals(0l, vertexId);
		logger.info("Data 1 [{}]", vertexId);

		// second
		vertexId2 = dataGraph.makeVertex(createDataRecord2(), graph, graphIndex);
		logger.info("Data 2 [{}]", vertexId2);
		assertNotEquals(0l, vertexId2);

		// third
		vertexId3 = dataGraph.makeVertex(createDataRecord3(), graph, graphIndex);
		logger.info("Data 3 [{}]", vertexId3);
		assertNotEquals(0l, vertexId3);

		// fourth
		vertexId4 = dataGraph.makeVertex(createDataRecord4(), graph, graphIndex);
		logger.info("Data 4 [{}]", vertexId4);
		assertNotEquals(0l, vertexId4);

		/**
		 * algorithm
		 */
		// create first algorithm vertex
		algorithmVertexId = algorithmGraph.makeVertex(createAlgorithmRecord1(), graph, graphIndex);
		assertNotEquals(0l, algorithmVertexId);
		logger.info("algorithm 1 [{}]", algorithmVertexId);

		// create second algorithm vertex
		algorithmVertexId2 = algorithmGraph.makeVertex(createAlgorithmRecord2(), graph, graphIndex);
		assertNotEquals(0l, algorithmVertexId2);
		logger.info("algorithm 2 [{}]", algorithmVertexId2);

		// create second algorithm vertex
		algorithmVertexId3 = algorithmGraph.makeVertex(createAlgorithmRecord3(), graph, graphIndex);
		assertNotEquals(0l, algorithmVertexId3);
		logger.info("algorithm 3 [{}]", algorithmVertexId3);

		algorithmVertexId4 = algorithmGraph.makeVertex(createAlgorithmRecord4(), graph, graphIndex);
		assertNotEquals(0l, algorithmVertexId4);
		logger.info("algorithm 4 [{}]", algorithmVertexId4);

		AlgorithmNode record = createAlgorithmRecord4();
		record.setUid("987492874957");
		record.setInput(null);
		algorithmVertexId5 = algorithmGraph.makeVertex(record, graph, graphIndex);
		assertNotEquals(0l, algorithmVertexId5);
		logger.info("algorithm 5 [{}]", algorithmVertexId5);

		AlgorithmNode record1 = createAlgorithmRecord4();
		record1.setUid("0983275498273");
		record1.getInput().get(0).setType(null);
		record1.getInput().get(1).setType(null);
		algorithmVertexId6 = algorithmGraph.makeVertex(record1, graph, graphIndex);
		assertNotEquals(0l, algorithmVertexId6);
		logger.info("algorithm 6 [{}]", algorithmVertexId6);
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

		// algorithm
		List<Vertex> readVertexalgorithmId1 = commonVertexCode.getVertexId(algorithmVertexId, graph);
		assertEquals(algorithmVertexId, readVertexalgorithmId1.get(0).id());

		List<Vertex> readVertexalgorithmId2 = commonVertexCode.getVertexId(algorithmVertexId2, graph);
		assertEquals(algorithmVertexId2, readVertexalgorithmId2.get(0).id());

		List<Vertex> readVertexalgorithmId3 = commonVertexCode.getVertexId(algorithmVertexId3, graph);
		assertEquals(algorithmVertexId3, readVertexalgorithmId3.get(0).id());

		List<Vertex> readVertexalgorithmId4 = commonVertexCode.getVertexId(algorithmVertexId4, graph);
		assertEquals(algorithmVertexId4, readVertexalgorithmId4.get(0).id());

		List<Vertex> readVertexalgorithmId5 = commonVertexCode.getVertexId(algorithmVertexId5, graph);
		assertEquals(algorithmVertexId5, readVertexalgorithmId5.get(0).id());

	}

	@Test
	public void testDefineRelationship(){
		logger.info("Running testDefineRelationship...");
		List<Vertex> outGoingVertexId = commonVertexCode.getOutGoingToVertices(algorithmVertexId, graph);
		outGoingVertexId.forEach(vertex -> {
			logger.info("Vertex id [{}] and Vertex name [{}] ", vertex.id(), vertex.value("name"));
		});
		assertEquals(2, outGoingVertexId.size());

		List<Vertex> outGoingVertexId2 = commonVertexCode.getOutGoingToVertices(algorithmVertexId2, graph);
		outGoingVertexId2.forEach(vertex -> {
			logger.info("Vertex id [{}] and Vertex name [{}] ", vertex.id(), vertex.value("name"));
		});
		assertEquals(2, outGoingVertexId2.size());

		List<Vertex> outIngoingVertexId = commonVertexCode.getIncomingVertices(algorithmVertexId3, graph);
		outIngoingVertexId.forEach(vertex -> {
			logger.info("Vertex id [{}] and Vertex name [{}] ", vertex.id(), vertex.value("name"));
		});
		assertEquals(2, outIngoingVertexId.size());

		List<Vertex> outGoingVertexId5 = commonVertexCode.getOutGoingToVertices(algorithmVertexId5, graph);
		assertEquals(0, outGoingVertexId5.size());

		List<Vertex> incomingVertexId5 = commonVertexCode.getIncomingVertices(algorithmVertexId5, graph);
		assertEquals(0, incomingVertexId5.size());

		List<Vertex> outGoingVertexId6 = commonVertexCode.getOutGoingToVertices(algorithmVertexId6, graph);
		assertEquals(0, outGoingVertexId6.size());
	}

	@Test
	public void testUpdateVertices() throws NoSuchElementException, JSONException, ExampleTopologyException{
		logger.info("Running updateVertices...");

		AlgorithmNode an1 = createAlgorithmRecord1();
		an1.setOperation(Operation.UPDATE);
		an1.setDescription("This test1 update for analytic record");
		NodeConfig config = an1.getInput().get(0);
		config.setType("com.coffeetechgaff.storm.algorithmnode.example");
		List<NodeConfig> configList = new ArrayList<>();
		configList.add(config);
		an1.setInput(configList);
		long updatedVertexId1 = algorithmGraph.makeVertex(an1, graph, graphIndex);
		assertEquals(algorithmVertexId, updatedVertexId1);
		List<Vertex> v1 = commonVertexCode.getVertexId(updatedVertexId1, graph);
		assertEquals(1, v1.size());
		assertEquals("This test1 update for analytic record", v1.get(0).value("description"));
		long v1id = (long) v1.get(0).id();
		List<Vertex> outGoingVertex = commonVertexCode.getOutGoingToVertices(v1id, graph);
		assertEquals(0, outGoingVertex.size());
		List<Vertex> incomingVeretx = commonVertexCode.getIncomingVertices(v1id, graph);
		assertEquals(0, incomingVeretx.size());

		AlgorithmNode an2 = createAlgorithmRecord2();
		an2.setOperation(Operation.UPDATE);
		an2.setDescription("This is graph database");
		long updatedVertexId2 = algorithmGraph.makeVertex(an2, graph, graphIndex);
		assertEquals(algorithmVertexId2, updatedVertexId2);
		List<Vertex> v2 = commonVertexCode.getVertexId(updatedVertexId2, graph);
		assertEquals(1, v2.size());
		assertEquals("This is graph database", v2.get(0).value("description"));

		AlgorithmNode an3 = createAlgorithmRecord3();
		List<NodeConfig> inputConfig = an3.getInput();
		an3.setInput(null);
		an3.setOperation(Operation.UPDATE);
		an3.setDescription("This is graph database");
		long updatedVertexId3 = algorithmGraph.makeVertex(an3, graph, graphIndex);
		assertEquals(algorithmVertexId3, updatedVertexId3);
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

		AlgorithmNode an1 = createAlgorithmRecord1();
		an1.setOperation(Operation.DELETE);
		long deletedVertexId1 = algorithmGraph.makeVertex(an1, graph, graphIndex);
		assertEquals(algorithmVertexId, deletedVertexId1);

		AlgorithmNode an2 = createAlgorithmRecord2();
		an2.setOperation(Operation.DELETE);
		long deletedVertexId2 = algorithmGraph.makeVertex(an2, graph, graphIndex);
		assertEquals(algorithmVertexId2, deletedVertexId2);

		// checking to make sure the vertices has been deleted
		List<Vertex> v1 = commonVertexCode.getVertexId(deletedVertexId1, graph);
		assertTrue(v1.isEmpty());
		List<Vertex> v2 = commonVertexCode.getVertexId(deletedVertexId2, graph);
		assertTrue(v2.isEmpty());
	}

	@Test
	public void testOperationError() throws ExampleTopologyException{
		logger.info("Running testOperationError...");
		AlgorithmNode ar = createAlgorithmRecord1();
		ar.setOperation(Operation.valueOf("WRONG"));
		long wrongVertexId = algorithmGraph.makeVertex(ar, graph, graphIndex);
		assertEquals(0l, wrongVertexId);
	}

	@Test(expected = VertexNotFoundException.class)
	public void updateVerticesThatDoesntExists() throws ExampleTopologyException{
		logger.info("Running updateVerticesThatDoesntExists...");
		AlgorithmNode ds5 = createAlgorithmRecord1();
		ds5.setUid("4958734927");
		ds5.setOperation(Operation.UPDATE);
		ds5.setDescription("This is test3 update without dataTypes");
		algorithmGraph.makeVertex(ds5, graph, graphIndex);
	}

	@Test(expected = ValidatedMapEmptyException.class)
	public void testValidateMapUpdateIsEmpty() throws ExampleTopologyException{
		logger.info("Running testValidateMapUpdateIsEmpty...");
		AlgorithmNode anr = new AlgorithmNode();
		anr.setAuthor("");
		anr.setEmail("");
		anr.setOperation(Operation.UPDATE);
		algorithmGraph.makeVertex(anr, graph, graphIndex);
	}

	@Test(expected = ObjectIsNullException.class)
	public void testCreateObjectisNull() throws ExampleTopologyException{
		logger.info("Running testCreateObjectisNull...");
		algorithmGraph.createVertex(null);
	}

	@Test(expected = ValidatedMapEmptyException.class)
	public void testValidateMapCreateIsEmpty() throws ExampleTopologyException{
		logger.info("Running testValidateMapCreateIsEmpty...");
		AlgorithmNode anr = new AlgorithmNode();
		anr.setAuthor("");
		anr.setEmail("");
		anr.setOperation(Operation.CREATE);
		algorithmGraph.makeVertex(anr, graph, graphIndex);
	}

	@Test
	public void testGraphClose(){
		commonVertexCode.closeGraph(null);
	}

	private DataNode createDataRecord1(){
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

	private DataNode createDataRecord2(){
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

	private DataNode createDataRecord3(){
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

	private DataNode createDataRecord4(){
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

	private AlgorithmNode createAlgorithmRecord1(){
		return ExampleTopologyCommonTestUtils.getAlgorithmObject();
	}

	private AlgorithmNode createAlgorithmRecord4(){
		return ExampleTopologyCommonTestUtils.getAlgorithmObject2();
	}

	private AlgorithmNode createAlgorithmRecord2(){
		return ExampleTopologyCommonTestUtils.getAlgorithmObject3();
	}

	private AlgorithmNode createAlgorithmRecord3(){
		return ExampleTopologyCommonTestUtils.getAlgorithmObject4();
	}

}
