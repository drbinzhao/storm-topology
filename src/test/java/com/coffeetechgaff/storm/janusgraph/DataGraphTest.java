package com.coffeetechgaff.storm.janusgraph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

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
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coffeetechgaff.storm.algorithmnode.AlgorithmNode;
import com.coffeetechgaff.storm.datanode.DataNode;
import com.coffeetechgaff.storm.datanode.Operation;
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

/**
 * 
 * @author VivekSubedi
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DataGraphTest{

	private static Logger logger = LoggerFactory.getLogger(DataGraphTest.class);

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
	private static long vertexId5 = 0l;
	private static long vertexId6 = 0l;

	// algorithm vertex ID
	private static long algorithmVertexId = 0l;
	private static long algorithmVertexId2 = 0l;
	private static long algorithmVertexId3 = 0l;
	private static long algorithmVertexId4 = 0l;

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
	public void testNullObjects() throws ExampleTopologyException{
		logger.info("Running testNullObjects...");
		dataGraph.makeVertex(null, graph, graphIndex);
	}

	@Test(expected = ObjectIsNullException.class)
	public void testNullGraphObjects() throws ExampleTopologyException{
		logger.info("Running testNullGraphObjects...");
		dataGraph.makeVertex(createDataRecord1(), null, graphIndex);
	}

	@Test(expected = ObjectIsNullException.class)
	public void testNullIndexObjects() throws ExampleTopologyException{
		logger.info("Running testNullIndexObjects...");
		dataGraph.makeVertex(createDataRecord1(), graph, null);
	}

	@Test
	public void createVertexes() throws ExampleTopologyException{
		logger.info("Running createVertexes...");
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
		logger.info("algorithm 3 [{}]", algorithmVertexId4);

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

		DataNode ds = createDataRecord4();
		ds.setDataTypes(null);
		vertexId5 = dataGraph.makeVertex(ds, graph, graphIndex);
		assertNotEquals(0l, vertexId5);
		logger.info("Data 5 [{}]", vertexId5);

		ds.setDataTypes(new ArrayList<>());
		vertexId6 = dataGraph.makeVertex(ds, graph, graphIndex);
		assertNotEquals(0l, vertexId6);
		logger.info("Data 6 [{}]", vertexId6);
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

		// algorithm
		List<Vertex> readVertexalgorithmId1 = commonVertexCode.getVertexId(algorithmVertexId, graph);
		assertEquals(algorithmVertexId, readVertexalgorithmId1.get(0).id());

		List<Vertex> readVertexalgorithmId2 = commonVertexCode.getVertexId(algorithmVertexId2, graph);
		assertEquals(algorithmVertexId2, readVertexalgorithmId2.get(0).id());

		List<Vertex> readVertexalgorithmId3 = commonVertexCode.getVertexId(algorithmVertexId3, graph);
		assertEquals(algorithmVertexId3, readVertexalgorithmId3.get(0).id());
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

		List<Vertex> outGoingAnalyticVertexIds = commonVertexCode.getOutGoingToVertices(algorithmVertexId, graph);
		assertEquals(2, outGoingAnalyticVertexIds.size());
		List<Vertex> outGoingAnalyticVertexIds2 = commonVertexCode.getOutGoingToVertices(algorithmVertexId2, graph);
		assertEquals(2, outGoingAnalyticVertexIds2.size());
		List<Vertex> outGoingAnalyticVertexIds3 = commonVertexCode.getOutGoingToVertices(algorithmVertexId3, graph);
		assertEquals(2, outGoingAnalyticVertexIds3.size());
		List<Vertex> outGoingAnalyticVertexIds4 = commonVertexCode.getOutGoingToVertices(algorithmVertexId4, graph);
		assertEquals(2, outGoingAnalyticVertexIds4.size());
	}

	@Test
	public void updateVertices() throws ExampleTopologyException{
		logger.info("Running updateVertices...");
		// first
		DataNode ds1 = createDataRecord1();
		ds1.setOperation(Operation.UPDATE);
		ds1.setDescription("This is test1 update");
		ds1.setDataTypes(Arrays.asList(new String[]{"com.coffeetechgaff.storm.algorithmnode.example"}));
		long updatedVertexId1 = dataGraph.makeVertex(ds1, graph, graphIndex);
		assertEquals(vertexId, updatedVertexId1);
		List<Vertex> v1 = commonVertexCode.getVertexId(updatedVertexId1, graph);
		assertEquals(1, v1.size());
		assertEquals("This is test1 update", v1.get(0).value("description"));
		long v1id = (long) v1.get(0).id();
		List<Vertex> outGoingVertex = commonVertexCode.getOutGoingToVertices(v1id, graph);
		assertEquals(0, outGoingVertex.size());
		List<Vertex> incomingVeretx = commonVertexCode.getIncomingVertices(v1id, graph);
		assertEquals(0, incomingVeretx.size());

		// second
		DataNode ds2 = createDataRecord2();
		ds2.setOperation(Operation.UPDATE);
		ds2.setDescription("This is test2 update");
		long updatedVertexId2 = dataGraph.makeVertex(ds2, graph, graphIndex);
		assertEquals(vertexId2, updatedVertexId2);
		List<Vertex> v2 = commonVertexCode.getVertexId(updatedVertexId2, graph);
		assertEquals(1, v2.size());
		assertEquals("This is test2 update", v2.get(0).value("description"));

		// third
		DataNode ds3 = createDataRecord3();
		ds3.setDataTypes(null);
		ds3.setOperation(Operation.UPDATE);
		ds3.setDescription("This is test3 update with null datatypes");
		long updatedVertexId3 = dataGraph.makeVertex(ds3, graph, graphIndex);
		logger.info("Dataosurce 3 [{}]", updatedVertexId3);
		assertEquals(vertexId3, updatedVertexId3);
		List<Vertex> v3 = commonVertexCode.getVertexId(updatedVertexId3, graph);
		assertEquals(1, v3.size());
		assertEquals("This is test3 update with null datatypes", v3.get(0).value("description"));

		// fourth
		DataNode ds4 = createDataRecord3();
		ds4.setDataTypes(new ArrayList<>());
		ds4.setOperation(Operation.UPDATE);
		ds4.setDescription("This is test3 update without dataTypes");
		long updatedVertexId4 = dataGraph.makeVertex(ds4, graph, graphIndex);
		logger.info("Dataosurce 4 [{}]", updatedVertexId4);
		assertEquals(vertexId3, updatedVertexId4);
		List<Vertex> v4 = commonVertexCode.getVertexId(updatedVertexId4, graph);
		assertEquals(1, v4.size());
		assertEquals("This is test3 update without dataTypes", v4.get(0).value("description"));
	}

	@Test
	public void testOperationError() throws ExampleTopologyException{
		DataNode ar = createDataRecord1();
		ar.setOperation(Operation.valueOf("WRONG"));
		long wrongVertexId = dataGraph.makeVertex(ar, graph, graphIndex);
		assertEquals(0l, wrongVertexId);
	}

	@Test
	public void updateVerticesWithDeleteOparation() throws ExampleTopologyException{
		logger.info("Running updateVerticesWithDeleteOparation...");
		// first
		DataNode ds1 = createDataRecord1();
		ds1.setOperation(Operation.DELETE);
		long deletedVertexId1 = dataGraph.makeVertex(ds1, graph, graphIndex);
		assertEquals(vertexId, deletedVertexId1);

		// second
		DataNode ds2 = createDataRecord2();
		ds2.setOperation(Operation.DELETE);
		long deletedVertexId2 = dataGraph.makeVertex(ds2, graph, graphIndex);
		assertEquals(vertexId2, deletedVertexId2);

		// third
		DataNode ds3 = createDataRecord3();
		ds3.setOperation(Operation.DELETE);
		long deletedVertexId3 = dataGraph.makeVertex(ds3, graph, graphIndex);
		assertEquals(vertexId3, deletedVertexId3);

		// checking to make sure the vertices has been deleted
		List<Vertex> v1 = commonVertexCode.getVertexId(deletedVertexId1, graph);
		assertTrue(v1.isEmpty());
		List<Vertex> v2 = commonVertexCode.getVertexId(deletedVertexId2, graph);
		assertTrue(v2.isEmpty());
		List<Vertex> v3 = commonVertexCode.getVertexId(deletedVertexId2, graph);
		assertTrue(v3.isEmpty());
	}

	@Test(expected = VertexNotFoundException.class)
	public void updateVerticesThatDoesntExists() throws ExampleTopologyException{
		DataNode ds5 = createDataRecord3();
		ds5.setId("4958734927");
		ds5.setDataTypes(new ArrayList<>());
		ds5.setOperation(Operation.UPDATE);
		ds5.setDescription("This is test3 update without dataTypes");
		dataGraph.makeVertex(ds5, graph, graphIndex);
	}

	@Test(expected = ValidatedMapEmptyException.class)
	public void testValidateMapUpdateIsEmpty() throws ExampleTopologyException{
		logger.info("Running testValidateMapUpdateIsEmpty...");
		DataNode anr = new DataNode();
		anr.setClassification("");
		anr.setName("");
		anr.setOperation(Operation.UPDATE);
		dataGraph.makeVertex(anr, graph, graphIndex);
	}

	@Test(expected = ObjectIsNullException.class)
	public void testCreateObjectisNull() throws ExampleTopologyException{
		logger.info("Running testCreateObjectisNull...");
		dataGraph.createVertex(null);
	}

	@Test(expected = ValidatedMapEmptyException.class)
	public void testValidateMapCreateIsEmpty() throws ExampleTopologyException{
		logger.info("Running testValidateMapCreateIsEmpty...");
		DataNode anr = new DataNode();
		anr.setClassification("");
		anr.setName("");
		anr.setOperation(Operation.CREATE);
		dataGraph.makeVertex(anr, graph, graphIndex);
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
		return ExampleTopologyCommonTestUtils.getAnalyticDefinationObject();
	}

	private AlgorithmNode createAlgorithmRecord4(){
		return ExampleTopologyCommonTestUtils.getAnalyticDefinationObject2();
	}

	private AlgorithmNode createAlgorithmRecord2(){
		return ExampleTopologyCommonTestUtils.getAnalyticDefinationObject3();
	}

	private AlgorithmNode createAlgorithmRecord3(){
		return ExampleTopologyCommonTestUtils.getAnalyticDefinationObject4();
	}

}
