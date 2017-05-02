package com.coffeetechgaff.storm.utils;

import java.util.Arrays;
import java.util.List;

/**
 * 
 * @author VivekSubedi
 *
 */
public class ExampleTopologyUtils{

	// constant variables that can be access through out application
	public static final String OPERATION_CREATE = "CREATE";
	public static final String OPERATION_UPDATE = "UPDATE";
	public static final String OPERATION_DELETE = "DELETE";

	public static final Boolean EXTERNAL_CACHEDBCACHE = true;
	public static final Integer EXTERNAL_CACHEDBCACHECLEANWAIT = 20;
	public static final Long EXTERNAL_CACHEDBCACHETIME = 180000L;
	public static final Double EXTERNAL_CACHEDBCACHESIZE = 0.25;

	public static final String KAFKAZOOKEEPER = "kafka.zookeeper";
	public static final String KAFKAZKROOT = "kafka.zkRoot";
	public static final String STORMCONTENT = "content";
	public static final String ALGORITHMTOPIC = "algorithm.topic";
	public static final String DATATOPIC = "data.topic";
	public static final String STORAGEBACKEND = "storage.backend";
	public static final String STORAGEHOSTNAME = "storage.backend.host";
	public static final String INDEXSEARCHBACKEND = "index.search.backend";
	public static final String INDEXSEARCHHOSTNAME = "index.search.hostname";
	public static final String INDEXSEARCHCLIENTONLY = "index.search.client.only";
	public static final String GRAPHINDEX = "graph.index";
	public static final String SPOUTCOUNT = "spout.count";
	public static final String BOLTCOUNT = "bolt.count";
	public static final String WORKERTHREAD = "worker.thread";
	public static final String DBCACHE = "db.cache";
	public static final String DBCACHECLEANWAIT = "db.cache.clean.wait";
	public static final String DBCACHETIME = "db.cache.time";
	public static final String DBCACHESIZE = "db.cache.size";

	// following are constant over the application
	public static final String DATASTREAM = "data-stream";
	public static final String ALGORITHMSTREAM = "algorithm-stream";

	private ExampleTopologyUtils(){
		// private constructor
	}

	public static List<String> getTopologyList(){
		return Arrays.asList(new String[]{KAFKAZOOKEEPER, KAFKAZKROOT, ALGORITHMTOPIC, DATATOPIC, STORAGEBACKEND,
				STORAGEHOSTNAME, INDEXSEARCHBACKEND, INDEXSEARCHHOSTNAME, INDEXSEARCHCLIENTONLY, GRAPHINDEX,
				SPOUTCOUNT, BOLTCOUNT, WORKERTHREAD, DBCACHE, DBCACHECLEANWAIT, DBCACHETIME, DBCACHESIZE});
	}
}
