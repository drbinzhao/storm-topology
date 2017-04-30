package com.coffeetechgaff.storm.janusgraph;

import java.util.Map;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

/**
 * Class to initialize the connection to @JanusGraph
 * 
 * @author VivekSubedi
 *
 */
public class JanusGraphConnection{

	/**
	 * Creates a new instance of @JanusGraph with provided properties
	 * 
	 * @param configProperties
	 *            -@Map of user provided properties
	 * 
	 * @return-an instance of @JanusGraph
	 */
	public JanusGraph loadJanusGraph(Map<String, Object> configProperties){
		JanusGraphFactory.Builder config = JanusGraphFactory.build();
		if(configProperties.isEmpty()){
			throw new IllegalArgumentException("Properties map is not loaded");
		}

		for(Map.Entry<String, Object> entry : configProperties.entrySet()){
			config.set(entry.getKey(), entry.getValue());
		}

		return config.open();
	}
}
