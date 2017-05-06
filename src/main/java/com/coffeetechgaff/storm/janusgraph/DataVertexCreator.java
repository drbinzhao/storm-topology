package com.coffeetechgaff.storm.janusgraph;

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
import org.janusgraph.core.JanusGraphVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.coffeetechgaff.storm.datanode.DataNode;
import com.coffeetechgaff.storm.datanode.Operation;
import com.coffeetechgaff.storm.enumeration.CommonVertexLabelEnums;
import com.coffeetechgaff.storm.enumeration.EdgeLabelEnums;
import com.coffeetechgaff.storm.enumeration.MainVertexName;
import com.coffeetechgaff.storm.exception.ExampleTopologyException;
import com.coffeetechgaff.storm.exception.ObjectIsNullException;
import com.coffeetechgaff.storm.exception.ValidatedMapEmptyException;
import com.coffeetechgaff.storm.exception.VertexNotFoundException;
import com.coffeetechgaff.storm.validation.DataValidation;

/**
 * The class to make Datasource Vertex in graph database with provided avro
 * schema. It will create edge to all Analytic vertex if the data types matches
 * to define the realtionship between datasource and analytic
 * 
 * @author VivekSubedi
 *
 */
public class DataVertexCreator extends CommonVertexCode implements GraphVertex<DataNode>{

	private static final Logger logger = LoggerFactory.getLogger(DataVertexCreator.class);
	private static final String CANNOTBENULL = "can not be null";

	private JanusGraph graph;
	private GraphTraversalSource g;

	@Override
	public long makeVertex(DataNode vertexRecord, JanusGraph graphConnection, String index)
			throws ExampleTopologyException{

		// checking to sure objects are not null
		if(vertexRecord == null){
			throw new ObjectIsNullException("DataNode " + CANNOTBENULL);
		}

		if(graphConnection == null){
			throw new ObjectIsNullException("JanusGraph connection " + CANNOTBENULL);
		}

		if(StringUtils.isBlank(index)){
			throw new ObjectIsNullException("graphIndex " + CANNOTBENULL);
		}

		// setting up local instance of this graph
		this.graph = graphConnection;
		g = graph.traversal();

		if(vertexRecord.getOperation() == Operation.CREATE){
			// loading datasource properties
			super.loadProperties(graph, index);

			// creating vertex
			return createVertex(vertexRecord);
		}else if(vertexRecord.getOperation() == Operation.UPDATE){
			// updating the existing vertex
			return updateVertex(vertexRecord);
		}else if(vertexRecord.getOperation() == Operation.DELETE){
			// deleting the existing vertex
			return super.deleteVertex(vertexRecord.getId(), g);
		}else{
			logger.error(
					"The operation [{}] that is being send in request is not identified. Please provide the valid operation requests. Those are \n1. CREATE \n2. UPDATE \n3. DELETE",
					vertexRecord.getOperation());
			return 0L;
		}
	}

	/**
	 * Updates the vertex property of existing vertex
	 * 
	 * @param nodeRecord
	 *            -all the properties that is being updated in the form of @DataSourceNodeRecord
	 *            object
	 * @return @Vertex id
	 * @throws ValidatedMapEmptyException
	 * @throws VertexNotFoundException
	 *             - When no vertex is retrieved for data id
	 */
	private long updateVertex(DataNode vertexRecord) throws ValidatedMapEmptyException,
			VertexNotFoundException{
		DataValidation validation = new DataValidation();
		Map<String, String> validatedMap = validation.validateDatasource(vertexRecord);

		if(validatedMap.isEmpty()){
			throw new ValidatedMapEmptyException(
					"All attributes become null after running validation against DataNode object");
		}

		logger.info("[{}] attributes are loading with value", validatedMap.size());

		// creating a vertex
		Vertex vertex;
		try{
			vertex = g.V().has(CommonVertexLabelEnums.ID.getVertexLabelName(), vertexRecord.getId()).next();
		}catch(FastNoSuchElementException e){
			logger.error("[{}] data id vertex doesn't exit in directory ", vertexRecord.getId(), e);
			throw new VertexNotFoundException(vertexRecord.getId() + " data id veretex doesn't exist");
		}

		// removing property from vertex first
		validatedMap.forEach((k, v) -> {
			vertex.property(k).remove();
			g.tx().commit();
		});

		// looping through each key of map and updating vertex. This happens in
		// memory
		validatedMap.forEach(vertex::property);
		g.tx().commit();

		long vertexId = (long) vertex.id();

		logger.info("[{}] vertex has been updated with new properties.", vertexId);

		// checking if the data types are loaded or not
		if(validatedMap.containsKey(CommonVertexLabelEnums.DATATYPE.getVertexLabelName())){
			logger.info("DataTypes have been updated on this request. We are re-creating new relationship to the algorithmNode vertices.");
			// dropping old relationship when data type is updated
			List<Edge> edge = g.V(vertex).bothE(EdgeLabelEnums.WORKSWITH.getEdgeLabelName()).toList();
			if(!edge.isEmpty()){
				logger.info("successfully retrived [{}] edges for vertex [{}]", edge.size(), vertex.id());
				edge.forEach(e -> e.remove());
				g.tx().commit();
			}

			// creating new relationship
			String attributeName = CommonVertexLabelEnums.DATATYPE.getVertexLabelName();
			List<String> dataTypeList = vertexRecord.getDataTypes();

			// creating edge
			createEdge(attributeName, dataTypeList, vertexId);
		}

		return vertexId;
	}

	@Override
	public long createVertex(DataNode nodeRecord) throws ObjectIsNullException, ValidatedMapEmptyException{
		if(nodeRecord == null){
			throw new ObjectIsNullException("AlgorithmNode " + CANNOTBENULL);
		}
		DataValidation dataValidation = new DataValidation();
		Map<String, String> datasourceValidatedMap = dataValidation.validateDatasource(nodeRecord);

		if(datasourceValidatedMap.isEmpty()){
			throw new ValidatedMapEmptyException(
					"All attributes become null after running validation against DataNode object");
		}

		logger.info("[{}] attributes are loading with value", datasourceValidatedMap.size());

		// getting the jenus graph
		JanusGraphTransaction transaction = graph.newTransaction();

		// creating a vertex properties
		JanusGraphVertex vertex = transaction.addVertex(T.label, MainVertexName.DATANODE.getVertexLabelName());
		// looping through each key of map and updating vertex. This happens in
		// memory
		datasourceValidatedMap.forEach(vertex::property);

		logger.info("New Vertex has been created with id [{}] and with label datanode having datanode id [{}]",
				vertex.id(), nodeRecord.getId());

		transaction.commit();

		// retrieving attributename and datatype
		String attributeName = CommonVertexLabelEnums.DATATYPE.getVertexLabelName();
		List<String> dataTypeList = nodeRecord.getDataTypes();

		if(datasourceValidatedMap.containsKey(CommonVertexLabelEnums.DATATYPE.getVertexLabelName())){
			// creating edge
			createEdge(attributeName, dataTypeList, vertex.longId());
			return vertex.longId();
		}

		return vertex.longId();
	}

	private void createEdge(String attributeName, List<String> dataTypeList, long vertexId){

		logger.info("Looping through each data type and creating edge if algorithm node exists for that datatype {}",
				dataTypeList);
		dataTypeList.forEach(dataType -> {
			List<Vertex> allMatchVertices = g.V().hasLabel(MainVertexName.ALGORITHM.getVertexLabelName())
					.has(attributeName, textContains(dataType)).toList();
			logger.info("There are [{}] vertices that matches the datatype [{}] that we are looking for. They are: {}",
					allMatchVertices.size(), dataType, allMatchVertices);
			allMatchVertices.forEach(vertex -> {
				logger.info("Creating an edge between algorithm and data");
				defineRelationShip(vertexId, (Long) vertex.id());
			});
		});
		g.tx().commit();
	}

	@Override
	public void defineRelationShip(long dataSourceVertexId, long analyticsVertexId){
		Assert.notNull(analyticsVertexId, "analyticsVertexId " + CANNOTBENULL);
		Assert.notNull(dataSourceVertexId, "dataSourceVertexId " + CANNOTBENULL);

		Vertex analyticVertex = g.V(analyticsVertexId).next();
		Vertex datasourceVertex = g.V(dataSourceVertexId).next();

		// adding edges in both direction
		analyticVertex.addEdge(EdgeLabelEnums.WORKSWITH.getEdgeLabelName(), datasourceVertex);
		datasourceVertex.addEdge(EdgeLabelEnums.WORKSWITH.getEdgeLabelName(), analyticVertex);
		g.tx().commit();
		logger.info(
				"Edge with [works with] has been marked the relationship between datanode [{}] and algorithmnode [{}] and vice-versa",
				dataSourceVertexId, analyticsVertexId);
	}
}
