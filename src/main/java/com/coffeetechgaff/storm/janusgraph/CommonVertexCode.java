package com.coffeetechgaff.storm.janusgraph;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.coffeetechgaff.storm.enumeration.CommonVertexLabelEnums;
import com.coffeetechgaff.storm.enumeration.EdgeLabelEnums;
import com.coffeetechgaff.storm.enumeration.MainVertexName;

/**
 * The common class to execute search and create schema operation of graph
 * database
 * 
 * @author VivekSubedi
 *
 */
public class CommonVertexCode{

	private static final Logger logger = LoggerFactory.getLogger(CommonVertexCode.class);
	private static final String CANNOTBENULL = "can not be null";
	private static final String JANUSGRAPH = "JanusGraph ";
	private static final String VERTEXID = "VertexId ";

	private AtomicBoolean wasRun = new AtomicBoolean(false);
	private String byDataType = "byDataType";
	private String byName = "byName";
	private String byId = "byId";
	private String byAuthor = "byAuthor";

	/**
	 * Retrieves all vertices that are connected to a vertex with out going
	 * edges from a vertex that the user is looking from
	 * 
	 * @param vertexId
	 *            -User provided vertex id from which edge going out to connect
	 *            to other vertices
	 * @param graph
	 *            -Instance of JanusGraph
	 * @return @List of @org.apache.tinkerpop.gremlin.structure.Vertex
	 */
	public List<Vertex> getOutGoingToVertices(long vertexId, JanusGraph graph){
		Assert.notNull(vertexId, VERTEXID + CANNOTBENULL);
		Assert.notNull(graph, JANUSGRAPH + CANNOTBENULL);
		List<Vertex> vertexList = graph.traversal().V(vertexId).out("works with").toList();
		logger.info("Vertex [{}] was connected to [{}]", vertexId, vertexList);
		return vertexList;
	}

	/**
	 * Retrieves all vertices that are connected to a vertex with incoming edges
	 * to a vertex that the user is looking at
	 * 
	 * @param vertexId
	 *            -User provided vertex id in which edges are coming in from
	 *            other vertices
	 * @param graph
	 *            -Instance of JanusGraph
	 * @return @List of @org.apache.tinkerpop.gremlin.structure.Vertex
	 */
	public List<Vertex> getIncomingVertices(long vertexId, JanusGraph graph){
		Assert.notNull(vertexId, VERTEXID + CANNOTBENULL);
		Assert.notNull(graph, JANUSGRAPH + CANNOTBENULL);
		List<Vertex> vertexList = graph.traversal().V(vertexId).in("works with").toList();
		logger.info("vertex [{}] was connected to [{}]", vertexId, vertexList);
		return vertexList;
	}

	/**
	 * Creates a schema in graph database
	 * 
	 * @param graph
	 *            -Instance of JanusGraph
	 * @param graphIndex
	 */
	public synchronized void loadProperties(JanusGraph graph, String graphIndex){
		Assert.notNull(graph, JANUSGRAPH + CANNOTBENULL);
		Assert.notNull(graphIndex, "graphIndex " + CANNOTBENULL);
		// using for thread safety and should only execute only once which ever
		// thread comes first
		if(!wasRun.getAndSet(true)){
			JanusGraphManagement mgmt = graph.openManagement();

			if(!mgmt.containsGraphIndex(graphIndex)){
				logger.info("Index has not been created. Creating new Index");
				// creating property key of datasource
				PropertyKey name = mgmt.makePropertyKey(CommonVertexLabelEnums.NAME.getVertexLabelName())
						.dataType(String.class).make();
				PropertyKey id = mgmt.makePropertyKey(CommonVertexLabelEnums.ID.getVertexLabelName())
						.dataType(String.class).make();
				mgmt.makePropertyKey(CommonVertexLabelEnums.CLASSIFICATION.getVertexLabelName()).dataType(String.class)
						.make();
				mgmt.makePropertyKey(CommonVertexLabelEnums.DESCRIPTION.getVertexLabelName()).dataType(String.class)
						.make();
				mgmt.makePropertyKey(CommonVertexLabelEnums.MATURITY.getVertexLabelName()).dataType(String.class)
						.make();
				mgmt.makePropertyKey(CommonVertexLabelEnums.STATUS.getVertexLabelName()).dataType(String.class).make();
				PropertyKey author = mgmt.makePropertyKey(CommonVertexLabelEnums.AUTHOR.getVertexLabelName())
						.dataType(String.class).make();
				mgmt.makePropertyKey(CommonVertexLabelEnums.VERSION.getVertexLabelName()).dataType(String.class).make();
				mgmt.makePropertyKey(CommonVertexLabelEnums.EMAIL.getVertexLabelName()).dataType(String.class).make();
				PropertyKey dataType = mgmt.makePropertyKey(CommonVertexLabelEnums.DATATYPE.getVertexLabelName())
						.dataType(String.class).cardinality(Cardinality.LIST).make();
				mgmt.makePropertyKey(CommonVertexLabelEnums.INPUT.getVertexLabelName()).dataType(String.class).make();
				mgmt.makePropertyKey(CommonVertexLabelEnums.OUTPUT.getVertexLabelName()).dataType(String.class).make();
				mgmt.makePropertyKey(CommonVertexLabelEnums.PARAMETERS.getVertexLabelName()).dataType(String.class)
						.make();

				// creating composite key for the search
				mgmt.buildIndex(graphIndex, Vertex.class).addKey(dataType).addKey(name).addKey(author).addKey(id)
						.buildCompositeIndex();
				mgmt.buildIndex(byDataType, Vertex.class).addKey(dataType).buildCompositeIndex();
				mgmt.buildIndex(byName, Vertex.class).addKey(name).buildCompositeIndex();
				mgmt.buildIndex(byId, Vertex.class).addKey(id).buildCompositeIndex();
				mgmt.buildIndex(byAuthor, Vertex.class).addKey(author).buildCompositeIndex();

				// defining vertices
				mgmt.makeVertexLabel(MainVertexName.DATANODE.getVertexLabelName()).make();
				mgmt.makeVertexLabel(MainVertexName.ALGORITHM.getVertexLabelName()).make();

				// making edge label
				mgmt.makeEdgeLabel(EdgeLabelEnums.WORKSWITH.getEdgeLabelName()).multiplicity(Multiplicity.MULTI).make();

				// commit the properties and labels
				mgmt.commit();
			}else{
				logger.info("Index has been created already. Not doing anything!");
			}
		}

	}

	/**
	 * This is only for testing read back vertices
	 * 
	 * @param vertexId
	 * @param graph
	 * @return
	 */
	public List<Vertex> getVertexId(long vertexId, JanusGraph graph){
		Assert.notNull(vertexId, VERTEXID + CANNOTBENULL);
		Assert.notNull(graph, JANUSGRAPH + CANNOTBENULL);
		List<Vertex> vertexList = new ArrayList<>();
		GraphTraversal<Vertex, Vertex> iterator = graph.traversal().V(vertexId);
		if(graph.traversal().V(vertexId).hasNext()){
			vertexList.add(iterator.next());
		}
		return vertexList;
	}

	/**
	 * //g.V(vertex.id()).drop();
	 * 
	 * @param dataId
	 *            - data id of a vertex
	 * @param g
	 *            - @GraphTraversalSource instance of a graph
	 * @return deleted vertex Id
	 */
	public long deleteVertex(String dataId, GraphTraversalSource g){
		Assert.notNull(dataId, "dataId " + CANNOTBENULL);
		Assert.notNull(g, "GraphTraversalSource " + CANNOTBENULL);
		logger.info("Deleting vertex and its relationship of data id [{}]", dataId);
		Vertex vertex = g.V().has(CommonVertexLabelEnums.ID.getVertexLabelName(), dataId).next();
		vertex.remove();
		g.tx().commit();
		long deletedVertexId = (long) vertex.id();
		logger.info("[{}] vertex has been deleted from our graph database with data id [{}]", deletedVertexId, dataId);
		return deletedVertexId;
	}

	/**
	 * Closing the graph instance
	 *
	 * @param graph
	 */
	public void closeGraph(JanusGraph graph){
		if(graph != null){
			graph.close();
		}
	}
}
