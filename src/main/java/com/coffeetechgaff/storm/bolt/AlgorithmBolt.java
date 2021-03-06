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

import com.coffeetechgaff.storm.algorithmnode.AlgorithmNode;
import com.coffeetechgaff.storm.utils.ExampleTopologyUtils;

/**
 * The bolt consumes message that is send from KafaSpout and deserialize to @AlgorithmNode
 * and emits the newly created object to analytic-stream so that GraphBolt can
 * pick it up for further processing
 * 
 * @author VivekSubedi
 *
 */
public class AlgorithmBolt extends BaseRichBolt{

	private static final Logger logger = LoggerFactory.getLogger(AlgorithmBolt.class);
	private static final long serialVersionUID = 3641070103475509670L;

	private transient DatumReader<AlgorithmNode> reader;
	private transient OutputCollector collector;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector){
		reader = new SpecificDatumReader<>(AlgorithmNode.class);
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input){
		byte[] value = input.getBinary(0);
		AlgorithmNode algorithmNode = deserialize(value);
		if(algorithmNode != null){
			collector.emit(ExampleTopologyUtils.ALGORITHMSTREAM, new Values(algorithmNode));
			logger.info("Emitted value under [{}] is [{}] and send to graph bolt", ExampleTopologyUtils.ALGORITHMSTREAM,
					algorithmNode);
		}
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer){
		declarer.declareStream(ExampleTopologyUtils.ALGORITHMSTREAM, new Fields(ExampleTopologyUtils.STORMCONTENT));
	}

	public AlgorithmNode deserialize(byte[] message){
		AlgorithmNode algorithm = null;
		try{
			Decoder decoder = DecoderFactory.get().binaryDecoder(message, null);
			/**
			 * We don't have to check if the read object is instance of
			 * @AlgorithmNode or not because read variable know that we are
			 * reading the byte of @AlgorithmNode. If serialized object is
			 * other than @AlgorithmNode, read.write throws the IOException.
			 */
			algorithm = reader.read(null, decoder);
		}catch(IOException | RuntimeException e){
			logger.error("Throwing exception on analytic deserialization ", e);
			logger.error("Something went wrong while deseralizing analytics byte arrays. Please check the avro schema. returning null");
			return algorithm;
		}
		return algorithm;
	}

}
