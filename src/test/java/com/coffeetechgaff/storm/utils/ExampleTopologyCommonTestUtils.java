package com.coffeetechgaff.storm.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;

import com.coffeetechgaff.storm.algorithmnode.AlgorithmNode;
import com.coffeetechgaff.storm.algorithmnode.AlgorithmParameter;
import com.coffeetechgaff.storm.algorithmnode.NodeConfig;
import com.coffeetechgaff.storm.algorithmnode.NodeStatus;
import com.coffeetechgaff.storm.algorithmnode.Operation;
import com.coffeetechgaff.storm.datanode.DataNode;
import com.coffeetechgaff.storm.utils.ExampleTopologyUtils;

/**
 * Directory common test utils class to have common static methods
 * 
 * @author VivekSubedi
 *
 */
public class ExampleTopologyCommonTestUtils{

	public static final String KAFKAIP = "127.0.0.1";
	public static final String KAFKAPORT = "9092";
	public static final String ANALYTICTOPIC = "datasource";
	public static final String DATASOURCETOPIC = "algorithm";
	public static final String BROKERLIST = KAFKAIP + ":" + KAFKAPORT;
	public static final String CLIENTTYPE = "CONSUMER";
	public static final String GROUPID = "test";

	/**
	 * Serialized the @DataNode using @SpecificDatumWriter
	 * 
	 * @param record
	 *            - @DataNode
	 * @return An array of bytes
	 * @throws IOException
	 */
	public static byte[] serialize(DataNode record) throws IOException{
		SpecificDatumWriter<DataNode> fileWriter = new SpecificDatumWriter<>(DataNode.SCHEMA$);
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
		fileWriter.write(record, encoder);
		encoder.flush();
		byte[] serializedBytes = outputStream.toByteArray();
		return serializedBytes;
	}

	/**
	 * Creates the @DataSourceNodeRecord according to the Avro schema
	 * 
	 * @return an object of @DataSourceNodeRecord
	 */
	public static DataNode createDataRecord(){
		DataNode node = new DataNode();
		node.setClassification(null);
		node.setName("Geometry");
		node.setDataTypes(Arrays.asList(new String[]{"Geometry", "polygon"}));
		node.setDescription("This is for test");
		node.setMaturity("gold");
		node.setId("12345");
		node.setOperation(com.coffeetechgaff.storm.datanode.Operation.CREATE);
		return node;
	}

	/**
	 * Loads all the properties that is needed for @KafkaConsumer
	 * 
	 * @return @Propeties instance
	 */
	public static Properties loadCommonProperties(){
		Properties commonProperties = new Properties();
		commonProperties.put("topic", ExampleTopologyUtils.DATATOPIC);
		commonProperties.put("bootstrap.servers", "127.0.0.1:9092");
		commonProperties.put("client.type", "CONSUMER");
		commonProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		commonProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		commonProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		commonProperties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		commonProperties.put("group.id", "sample-group");
		return commonProperties;
	}

	public static AlgorithmNode getAlgorithmObject(){
		AlgorithmNode ad = new AlgorithmNode();
		List<NodeConfig> inputList = new ArrayList<>();
		NodeConfig sc = new NodeConfig();
		sc.setName("example");
		sc.setType("geometry");
		NodeConfig sc2 = new NodeConfig();
		sc2.setName("example2");
		sc2.setType("polygon");
		inputList.add(sc);
		inputList.add(sc2);
		ad.setInput(inputList);

		List<NodeConfig> output = new ArrayList<>();
		NodeConfig output1 = new NodeConfig();
		output1.setName("example");
		output1.setType("com.coffeetechgaff.storm.algorithmnode.GDELTRecord");
		output.add(output1);
		ad.setOutput(output);

		ad.setAuthor("test");
		ad.setDescription("this is a test1");
		ad.setInput(inputList);
		ad.setEmail("test@email.io");
		ad.setName("testing");
		ad.setUid("1273498213");
		ad.setStatus(NodeStatus.TESTING);
		ad.setVersion("1.0");
		ad.setOperation(Operation.CREATE);

		List<AlgorithmParameter> parameters = new ArrayList<>();
		AlgorithmParameter p1 = new AlgorithmParameter();
		p1.setDefaultValue("defaultValue");
		p1.setDescription("This is for test");
		p1.setEntity("truck");
		p1.setName("counting analytics");
		p1.setType("string");
		p1.setUiHint("bla");

		AlgorithmParameter p2 = new AlgorithmParameter();
		p2.setDefaultValue("defaultValue");
		p2.setDescription("This is for test");
		p2.setEntity("truck");
		p2.setName("counting analytics");
		p2.setType("string");
		p2.setUiHint("bla");

		parameters.add(p1);
		parameters.add(p2);

		ad.setParameters(parameters);

		return ad;
	}

	public static AlgorithmNode getAlgorithmObject2(){
		AlgorithmNode ad = new AlgorithmNode();
		List<NodeConfig> inputList = new ArrayList<>();
		NodeConfig sc = new NodeConfig();
		sc.setName("example");
		sc.setType("geometry");
		NodeConfig sc2 = new NodeConfig();
		sc2.setName("example2");
		sc2.setType("polygon");
		inputList.add(sc);
		inputList.add(sc2);
		ad.setInput(inputList);

		List<NodeConfig> output = new ArrayList<>();
		NodeConfig output1 = new NodeConfig();
		output1.setName("example");
		output1.setType("com.coffeetechgaff.storm.algorithmnode.analytic");
		output.add(output1);
		ad.setOutput(output);

		ad.setAuthor("vivek");
		ad.setDescription("this is a test2");
		ad.setInput(inputList);
		ad.setEmail("vivek@email.io");
		ad.setName("testing");
		ad.setUid("6728347");
		ad.setStatus(NodeStatus.TESTING);
		ad.setVersion("1.0");
		ad.setOperation(Operation.CREATE);

		List<AlgorithmParameter> parameters = new ArrayList<>();
		AlgorithmParameter p1 = new AlgorithmParameter();
		p1.setDefaultValue("defaultValue");
		p1.setDescription("This is for test");
		p1.setEntity("truck");
		p1.setName("math analytics");
		p1.setType("string");
		p1.setUiHint("bla");

		AlgorithmParameter p2 = new AlgorithmParameter();
		p2.setDefaultValue("defaultValue");
		p2.setDescription("This is for test");
		p2.setEntity("truck");
		p2.setName("math analytics");
		p2.setType("string");
		p2.setUiHint("bla");

		parameters.add(p1);
		parameters.add(p2);

		ad.setParameters(parameters);

		return ad;
	}

	public static AlgorithmNode getAlgorithmObject3(){
		AlgorithmNode ad = new AlgorithmNode();
		List<NodeConfig> inputList = new ArrayList<>();
		NodeConfig sc = new NodeConfig();
		sc.setName("example");
		sc.setType("rectangle");
		NodeConfig sc2 = new NodeConfig();
		sc2.setName("example2");
		sc2.setType("hexagon");
		inputList.add(sc);
		inputList.add(sc2);
		ad.setInput(inputList);

		List<NodeConfig> output = new ArrayList<>();
		NodeConfig output1 = new NodeConfig();
		output1.setName("example");
		output1.setType("com.coffeetechgaff.storm.algorithmnode.common");
		output.add(output1);
		ad.setOutput(output);

		ad.setAuthor("tang");
		ad.setDescription("this is a test3");
		ad.setInput(inputList);
		ad.setEmail("tang@email.io");
		ad.setName("testing");
		ad.setUid("93847592847");
		ad.setStatus(NodeStatus.TESTING);
		ad.setVersion("1.0");
		ad.setOperation(Operation.CREATE);

		List<AlgorithmParameter> parameters = new ArrayList<>();
		AlgorithmParameter p1 = new AlgorithmParameter();
		p1.setDefaultValue("defaultValue");
		p1.setDescription("This is for test");
		p1.setEntity("truck");
		p1.setName("math analytics");
		p1.setType("string");
		p1.setUiHint("bla");

		AlgorithmParameter p2 = new AlgorithmParameter();
		p2.setDefaultValue("defaultValue");
		p2.setDescription("This is for test");
		p2.setEntity("truck");
		p2.setName("math analytics");
		p2.setType("string");
		p2.setUiHint("bla");

		parameters.add(p1);
		parameters.add(p2);

		ad.setParameters(parameters);

		return ad;
	}

	public static AlgorithmNode getAlgorithmObject4(){
		AlgorithmNode ad = new AlgorithmNode();
		List<NodeConfig> inputList = new ArrayList<>();
		NodeConfig sc = new NodeConfig();
		sc.setName("example");
		sc.setType("circle");
		NodeConfig sc2 = new NodeConfig();
		sc2.setName("example2");
		sc2.setType("triangle");
		inputList.add(sc);
		inputList.add(sc2);
		ad.setInput(inputList);

		List<NodeConfig> output = new ArrayList<>();
		NodeConfig output1 = new NodeConfig();
		output1.setName("example");
		output1.setType("com.coffeetechgaff.storm.algorithmnode.datanode");
		output.add(output1);
		ad.setOutput(output);

		ad.setAuthor("noddy");
		ad.setDescription("this is a test4");
		ad.setInput(inputList);
		ad.setEmail("ryan@email.io");
		ad.setName("testing");
		ad.setUid("oiuwq902385039");
		ad.setStatus(NodeStatus.TESTING);
		ad.setVersion("1.0");
		ad.setOperation(Operation.CREATE);

		List<AlgorithmParameter> parameters = new ArrayList<>();
		AlgorithmParameter p1 = new AlgorithmParameter();
		p1.setDefaultValue("defaultValue");
		p1.setDescription("This is for test");
		p1.setEntity("truck");
		p1.setName("math analytics");
		p1.setType("string");
		p1.setUiHint("bla");

		AlgorithmParameter p2 = new AlgorithmParameter();
		p2.setDefaultValue("defaultValue");
		p2.setDescription("This is for test");
		p2.setEntity("truck");
		p2.setName("math analytics");
		p2.setType("string");
		p2.setUiHint("bla");

		parameters.add(p1);
		parameters.add(p2);

		ad.setParameters(parameters);

		return ad;
	}
}
