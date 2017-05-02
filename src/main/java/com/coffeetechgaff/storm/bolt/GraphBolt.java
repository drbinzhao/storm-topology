package com.coffeetechgaff.storm.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.janusgraph.core.JanusGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coffeetechgaff.storm.algorithmnode.AlgorithmNode;
import com.coffeetechgaff.storm.datanode.DataNode;
import com.coffeetechgaff.storm.exception.ExampleTopologyException;
import com.coffeetechgaff.storm.janusgraph.AlgorithmVertexCreator;
import com.coffeetechgaff.storm.janusgraph.DataVertexCreator;
import com.coffeetechgaff.storm.janusgraph.GraphVertex;
import com.coffeetechgaff.storm.janusgraph.JanusGraphConnection;
import com.coffeetechgaff.storm.utils.ExampleTopologyUtils;

/**
 * Graph bolt to write incoming deserialized Kafka message of datanode and
 * algorithm of avro records to graph database using @JanusGraph API
 * 
 * @author VivekSubedi
 *
 */
public class GraphBolt extends BaseRichBolt{

	private static final Logger logger = LoggerFactory.getLogger(GraphBolt.class);

	private static final long serialVersionUID = -1969089647234501442L;

	/**
	 * local variables (all have transient because we don't want to serialized
	 * these varaibles {transient variables are never serialized in java})
	 */
	private transient GraphVertex<DataNode> datasourceGraph;
	private transient GraphVertex<AlgorithmNode> analyticGraph;
	private transient JanusGraph graph;
	private transient OutputCollector collector;

	// local variables
	private String storageBackend;
	private String storageBackendHost;
	private String indexBackend;
	private String indexHostName;
	private Boolean clientOnly;
	private Boolean dbCache;
	private Integer dbCacheCleanWait;
	private Long dbCacheTime;
	private Double dbCacheSize;

	// default index is vertices
	private String graphIndex = "vertices";

	@Override
	public void execute(Tuple input){
		if(ExampleTopologyUtils.DATASTREAM.equals(input.getSourceStreamId())){
			DataNode record = (DataNode) input.getValueByField(ExampleTopologyUtils.STORMCONTENT);
			logger.info("Writing datasource node to graph");
			long vertexId;
			try{
				vertexId = datasourceGraph.makeVertex(record, graph, graphIndex);
				logger.info("Vertex has been created/updated/deleted with id [{}] based on its operation", vertexId);
			}catch(ExampleTopologyException e){
				logger.error("Something went wrong while processing datasource node record ", e);
			}
			collector.ack(input);
		}else if(ExampleTopologyUtils.ALGORITHMSTREAM.equals(input.getSourceStreamId())){
			AlgorithmNode record = (AlgorithmNode) input.getValueByField(ExampleTopologyUtils.STORMCONTENT);
			logger.info("Writing analytic node to graph");
			long vertexId;
			try{
				vertexId = analyticGraph.makeVertex(record, graph, graphIndex);
				logger.info("Vertex has been created/updated/deleted with id [{}] based on its operation", vertexId);
			}catch(ExampleTopologyException e){
				logger.error("Something went wrong while processing analytic node record ", e);
			}
			collector.ack(input);
		}else{
			// this shouldn't happen
			throw new IllegalArgumentException("Emitted stream is not valid");
		}
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector){
		this.storageBackend = conf.get(ExampleTopologyUtils.STORAGEBACKEND).toString();
		this.storageBackendHost = conf.get(ExampleTopologyUtils.STORAGEHOSTNAME).toString();
		this.indexBackend = conf.get(ExampleTopologyUtils.INDEXSEARCHBACKEND).toString();
		this.indexHostName = conf.get(ExampleTopologyUtils.INDEXSEARCHHOSTNAME).toString();
		this.clientOnly = Boolean.valueOf(conf.get(ExampleTopologyUtils.INDEXSEARCHCLIENTONLY).toString());
		this.dbCache = Boolean.valueOf(conf.get(ExampleTopologyUtils.DBCACHE).toString());
		this.dbCacheCleanWait = Integer.valueOf(conf.get(ExampleTopologyUtils.DBCACHECLEANWAIT).toString());
		this.dbCacheTime = Long.valueOf(conf.get(ExampleTopologyUtils.DBCACHETIME).toString());
		this.dbCacheSize = Double.valueOf(conf.get(ExampleTopologyUtils.DBCACHESIZE).toString());
		this.graphIndex = conf.get(ExampleTopologyUtils.GRAPHINDEX).toString();
		this.collector = collector;
		datasourceGraph = new DataVertexCreator();
		analyticGraph = new AlgorithmVertexCreator();
		JanusGraphConnection graphConnection = new JanusGraphConnection();
		Map<String, Object> configProperties = buildPropertyMap();
		graph = graphConnection.loadJanusGraph(configProperties);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer){
		// nothing doing after this bolt so we are not streaming anything from
		// this bolt
	}

	private Map<String, Object> buildPropertyMap(){
		Map<String, Object> configProperties = new HashMap<>();
		configProperties.put("storage.backend", storageBackend);
		configProperties.put("storage.hostname", storageBackendHost);
		configProperties.put("cache.db-cache", dbCache);
		configProperties.put("cache.db-cache-clean-wait", dbCacheCleanWait);
		configProperties.put("cache.db-cache-time", dbCacheTime);
		configProperties.put("cache.db-cache-size", dbCacheSize);
		configProperties.put("index.search.backend", indexBackend);
		configProperties.put("index.search.hostname", indexHostName);
		configProperties.put("index.search.elasticsearch.client-only", clientOnly);
		return configProperties;
	}

	/**
	 * Closing @JanusGraph connection
	 */
	@Override
	public void cleanup(){
		graph.close();
	}
}
