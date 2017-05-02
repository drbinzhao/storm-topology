package com.coffeetechgaff.storm.bolt;

import java.io.IOException;
import java.util.Map;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coffeetechgaff.storm.datanode.DataNode;
import com.coffeetechgaff.storm.utils.ExampleTopologyUtils;

/**
 * The bolt consumes message that is send from KafaSpout and deserialize to @DataNode
 * and emits the newly created object to data-stream so that GraphBolt can
 * pick it up for further processing
 * 
 * @author VivekSubedi
 *
 */
public class DataBolt extends BaseRichBolt{

	private static final Logger logger = LoggerFactory.getLogger(DataBolt.class);

	private static final long serialVersionUID = 5204636940488686349L;

	private transient DatumReader<DataNode> reader;
	private transient OutputCollector collector;

	@Override
	public void execute(Tuple tupple){
		byte[] value = tupple.getBinary(0);
		DataNode dataSourceNode = deserialize(value);
		if(dataSourceNode != null){
			collector.emit(ExampleTopologyUtils.DATASTREAM, new Values(dataSourceNode));
			logger.info("Emitted value under [{}] is [{}] and send to graph bolt", ExampleTopologyUtils.DATASTREAM,
					dataSourceNode);
		}
		collector.ack(tupple);
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map config, TopologyContext context, OutputCollector collector){
		reader = new SpecificDatumReader<>(DataNode.class);
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer){
		declarer.declareStream(ExampleTopologyUtils.DATASTREAM, new Fields(ExampleTopologyUtils.STORMCONTENT));
	}

	/**
	 * Deserialize a array of bytes to the specific object
	 * 
	 * @param message
	 *            -Array of serialized byte
	 * @return @DataSourceNodeRecord
	 * 
	 *         DataSourceNodeRecord.class.isInstance(value)
	 */
	public DataNode deserialize(byte[] message){
		DataNode dataSourceNode = null;
		try{
			Decoder decoder = DecoderFactory.get().binaryDecoder(message, null);

			/**
			 * We don't have to check if the read object is instance of
			 * @DataNode or not because read variable know that we
			 * are reading the byte of @DataNode. If serialized object
			 * is other than @DataNode, read.write throws the
			 * IOException.
			 */
			dataSourceNode = reader.read(null, decoder);
		}catch(IOException | RuntimeException e){
			logger.error("Exception raised in datasource deseriazation: ", e);
			logger.error("Something went wrong while deseralizing datasource byte array. Please check the avro schema. returning null");
			return dataSourceNode;
		}
		return dataSourceNode;
	}

}
