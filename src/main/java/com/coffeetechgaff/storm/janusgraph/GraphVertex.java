package com.coffeetechgaff.storm.janusgraph;

import org.janusgraph.core.JanusGraph;

import com.coffeetechgaff.storm.exception.ExampleTopologyException;
import com.coffeetechgaff.storm.exception.ObjectIsNullException;
import com.coffeetechgaff.storm.exception.ValidatedMapEmptyException;

/**
 * Common interface which executes the operation against JanusGraph database.
 * Very simple class to execute all the required functions against the @JanusGrap
 * 
 * @author VivekSubedi
 *
 * @param <T>
 */
public interface GraphVertex<T> {

	/**
	 * Method to create a Vertex in @JanusGraph database. It will define schema
	 * and creates the vertex.
	 * 
	 * @param vertexRecord
	 *            -User provided object
	 * @param graph
	 *            -Initialized graph connection
	 * @param graphIndex
	 * @return vertex id
	 * @throws ObjectIsNullException
	 * @throws ExampleTopologyException
	 */
	long makeVertex(T vertexRecord, JanusGraph graph, String graphIndex) throws ExampleTopologyException;

	/**
	 * Creates a Vertex in @JanusGraph database. This method only works if
	 * connection has been set previously
	 * 
	 * @param nodeRecord
	 *            -User provided object
	 * @return vertex id
	 * @throws ValidatedMapEmptyException
	 */
	long createVertex(T nodeRecord) throws ExampleTopologyException;

	/**
	 * Defines the relationship between two vertex. It actually creates a edge
	 * between those two Vertex.
	 * 
	 * @param dataSourceVertexId
	 * @param analyticsVertexId
	 */
	void defineRelationShip(long dataSourceVertexId, long analyticsVertexId);
}
