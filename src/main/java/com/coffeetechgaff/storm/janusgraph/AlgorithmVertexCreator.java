package com.coffeetechgaff.storm.janusgraph;

import org.janusgraph.core.JanusGraphVertex;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.janusgraph.core.attribute.Text.textContains;

import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.coffeetechgaff.storm.algorithmnode.AlgorithmNode;
import com.coffeetechgaff.storm.algorithmnode.NodeConfig;
import com.coffeetechgaff.storm.algorithmnode.Operation;
import com.coffeetechgaff.storm.enumeration.CommonVertexLabelEnums;
import com.coffeetechgaff.storm.enumeration.EdgeLabelEnums;
import com.coffeetechgaff.storm.enumeration.MainVertexName;
import com.coffeetechgaff.storm.exception.ExampleTopologyException;
import com.coffeetechgaff.storm.exception.ObjectIsNullException;
import com.coffeetechgaff.storm.exception.ValidatedMapEmptyException;
import com.coffeetechgaff.storm.exception.VertexNotFoundException;
import com.coffeetechgaff.storm.validation.AlgorithmValidation;

/**
 * 
 * @author VivekSubedi
 *
 */
public class AlgorithmVertexCreator extends CommonVertexCode implements GraphVertex<AlgorithmNode>{

	private static final Logger logger = LoggerFactory.getLogger(AlgorithmVertexCreator.class);
	private static final String CANNOTBENULL = "can not be null";

	private JanusGraph graph;
	private GraphTraversalSource g;

	@Override
	public long makeVertex(AlgorithmNode record, JanusGraph graph, String graphIndex) throws ExampleTopologyException{

		// checking to sure objects are not null
		if(record == null){
			throw new ObjectIsNullException("AnalyitcNodeRecord " + CANNOTBENULL);
		}

		if(graph == null){
			throw new ObjectIsNullException("JanusGraph connection " + CANNOTBENULL);
		}

		if(StringUtils.isBlank(graphIndex)){
			throw new ObjectIsNullException("graphIndex " + CANNOTBENULL);
		}

		this.graph = graph;
		g = graph.traversal();

		if(record.getOperation() == Operation.CREATE){
			// loading datasource properties
			super.loadProperties(graph, graphIndex);

			// creating vertex
			return createVertex(record);
		}else if(record.getOperation() == Operation.UPDATE){
			// updating the existing vertex
			return updateVertex(record);
		}else if(record.getOperation() == Operation.DELETE){
			// deleting the existing vertex
			return super.deleteVertex(record.getUid(), g);
		}else{
			logger.error(
					"The operation [{}] that is being send in request is not identified. Please provide the valid operation requests. Those are \n1. CREATE \n2. UPDATE \n3. DELETE",
					record.getOperation());
			return 0L;
		}
	}

	/**
	 * Updates the vertex property of existing vertex
	 * 
	 * @param nodeRecord
	 *            -all the properties that is being updated in the form of @AnalyticNodeRecord
	 *            object
	 * @return @Vertex id
	 * @throws VertexNotFoundException
	 * @throws ValidatedMapEmptyException
	 */
	private long updateVertex(AlgorithmNode nodeRecord) throws VertexNotFoundException, ValidatedMapEmptyException{
		AlgorithmValidation algorithmValidation = new AlgorithmValidation();
		Map<String, String> validatedMap = algorithmValidation.validateAnalytic(nodeRecord);

		if(validatedMap.isEmpty()){
			throw new ValidatedMapEmptyException(
					"All attributes become null after running validation against AnalyticNodeRecord object");
		}

		logger.info("[{}] attributes are loading with value", validatedMap.size());
		List<String> dataTypes = new ArrayList<>();

		// making sure inputConfig is not null and is not empty
		if(validatedMap.containsKey(CommonVertexLabelEnums.INPUT.getVertexLabelName())){
			dataTypes = loadDataTypes(nodeRecord.getInput());
		}

		// retrieving the vertex bases on the data id of vertex
		Vertex updatedVertex;
		try{
			updatedVertex = g.V().has(CommonVertexLabelEnums.ID.getVertexLabelName(), nodeRecord.getUid()).next();
		}catch(FastNoSuchElementException e){
			logger.error("[{}] data id vertex doesn't exit in directory ", nodeRecord.getUid(), e);
			throw new VertexNotFoundException(nodeRecord.getUid() + " data id veretex doesn't exist");
		}

		// removing property from vertex first
		validatedMap.forEach((k, v) -> {
			updatedVertex.property(k).remove();
			g.tx().commit();
		});
		// looping through each key of map and updating vertex. This happens in
		// memory
		validatedMap.forEach(updatedVertex::property);
		g.tx().commit();
		
		long vertexId = (long) updatedVertex.id();

		logger.info("[{}] vertex has been updated with new properties.", vertexId);

		// checking if we able to retrieve dataTypes from record
		if(!dataTypes.isEmpty()){
			logger.info("DataTypes have been updated on this request. We are re-creating new relationship to the datasource vertices.");
			// dropping old relationship when data type is updated
			List<Edge> edge = g.V(updatedVertex).bothE(EdgeLabelEnums.WORKSWITH.getEdgeLabelName()).toList();
			if(!edge.isEmpty()){
				logger.info("successfully retrived [{}] edges for vertex [{}]", edge.size(), updatedVertex.id());
				edge.forEach(Edge::remove);
				g.tx().commit();
			}

			// creating new relationship
			String attributeName = CommonVertexLabelEnums.DATATYPE.getVertexLabelName();

			// creating edge
			createEdge(attributeName, dataTypes, vertexId);
		}

		return vertexId;
	}

	private List<String> loadDataTypes(List<NodeConfig> input){
		List<String> dataTypes = new ArrayList<>();
		input.forEach(i -> {
			if(StringUtils.isNotBlank(i.getType())){
				dataTypes.add(i.getType());
			}
		});

		return dataTypes;
	}

	@Override
	public long createVertex(AlgorithmNode analyticNode) throws ValidatedMapEmptyException, ObjectIsNullException{
		if(analyticNode == null){
			throw new ObjectIsNullException("AnalyitcNodeRecord " + CANNOTBENULL);
		}
		
		AlgorithmValidation algorithmValidation = new AlgorithmValidation();
		Map<String, String> analyticValidatedMap = algorithmValidation.validateAnalytic(analyticNode);

		if(analyticValidatedMap.isEmpty()){
			throw new ValidatedMapEmptyException(
					"All attributes become null after running validation against AnalyticNodeRecord object");
		}

		logger.info("[{}] attributes are loading with value", analyticValidatedMap.size());

		// parsing analyticDefinition to map to or node
		List<String> dataTypes = new ArrayList<>();

		// making sure inputConfig is not null and is not empty
		if(analyticValidatedMap.containsKey(CommonVertexLabelEnums.INPUT.getVertexLabelName())){
			dataTypes = loadDataTypes(analyticNode.getInput());
		}

		// adding data types to the validated map if the there is data types
		if(!dataTypes.isEmpty()){
			// adding the dataTypes
			analyticValidatedMap.put(CommonVertexLabelEnums.DATATYPE.getVertexLabelName(), dataTypes.toString());
		}

		// getting the jenus graph
		JanusGraphTransaction transaction = graph.newTransaction();

		// creating a vertex with provided properties properties
		JanusGraphVertex vertex = transaction.addVertex(T.label, MainVertexName.ALGORITHM.getVertexLabelName());

		// looping through each key of map and updating vertex. This happens in
		// memory
		analyticValidatedMap.forEach(vertex::property);

		logger.info("New Vertex has been created with id [{}] and with label analytic having analytics id [{}]",
				vertex.id(), analyticNode.getUid());

		// committing the vertex from memory to the disk
		transaction.commit();

		// checking if the dataTypes list is empty or not. We are creating edges
		// only if there is dataTypes
		if(!dataTypes.isEmpty()){
			logger.info("Creating new realtionship");
			// retrieving attributename and datatype
			String attributeName = CommonVertexLabelEnums.DATATYPE.getVertexLabelName();
			List<String> dataTypeList = dataTypes;

			// creating edge
			createEdge(attributeName, dataTypeList, vertex.longId());
		}

		return vertex.longId();
	}

	private void createEdge(String attributeName, List<String> dataTypeList, long vertexId){

		logger.info("Looping through each data type and creating edge if datasource node exists for that datatype {}",
				dataTypeList);
		dataTypeList.forEach(dataType -> {
			List<Vertex> allMatchVertices = g.V().hasLabel(MainVertexName.DATANODE.getVertexLabelName())
					.has(attributeName, textContains(dataType)).toList();
			logger.info("There are [{}] vertices that matches the datatype [{}] that we are looking for. They are: {}",
					allMatchVertices.size(), dataType, allMatchVertices);
			allMatchVertices.forEach(vertex -> {
				logger.info("Creating an edge between datasource and analytic vertices");
				defineRelationShip(vertexId, (Long) vertex.id());
			});
		});
		g.tx().commit();
	}

	@Override
	public void defineRelationShip(long analyticId, long datasourceId){
		Assert.notNull(analyticId, "analyticsVertexId " + CANNOTBENULL);
		Assert.notNull(datasourceId, "dataSourceVertexId " + CANNOTBENULL);

		Vertex analytic = g.V(analyticId).next();
		Vertex datasource = g.V(datasourceId).next();

		// adding edges in both direction
		analytic.addEdge(EdgeLabelEnums.WORKSWITH.getEdgeLabelName(), datasource);
		datasource.addEdge(EdgeLabelEnums.WORKSWITH.getEdgeLabelName(), analytic);
		g.tx().commit();
		logger.info(
				"Edge with [works with] has been marked the relationship between analytics [{}] and datasource [{}] and vice-versa",
				analyticId, datasourceId);
	}
}
