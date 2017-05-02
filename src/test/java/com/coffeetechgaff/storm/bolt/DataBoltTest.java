package com.coffeetechgaff.storm.bolt;

import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;

import org.apache.avro.io.DatumReader;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coffeetechgaff.storm.bolt.DataBolt;
import com.coffeetechgaff.storm.datanode.DataNode;
import com.coffeetechgaff.storm.utils.ExampleTopologyCommonTestUtils;
import com.coffeetechgaff.storm.utils.ExampleTopologyUtils;

/**
 * 
 * @author VivekSubedi
 *
 */
public class DataBoltTest{

	private static final Logger logger = LoggerFactory.getLogger(DataBoltTest.class);

	@InjectMocks
	@Spy
	private DataBolt dataBolt;

	@Mock
	private DatumReader<DataNode> reader;

	@Mock
	private OutputCollector collector;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception{
		logger.info("============== START UNIT TEST ==============");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception{
		logger.info("============== END UNIT TEST ==============");
	}

	@Before
	public void setUp() throws Exception{
		MockitoAnnotations.initMocks(this);
	}

	@Test
	public void testPrepare(){
		logger.info("Running testPrepare...");

		// mocking
		@SuppressWarnings("rawtypes")
		Map map = mock(Map.class);
		TopologyContext context = mock(TopologyContext.class);

		// calling method
		dataBolt.prepare(map, context, collector);

		// verifying the call
		verify(dataBolt, times(1)).prepare(map, context, collector);
	}

	@Test
	public void testExecute() throws IOException{
		logger.info("Running testExecute...");
		DataNode node = ExampleTopologyCommonTestUtils.createDataSourcenRecord();

		// mocking
		Tuple input = mock(Tuple.class);
		when(input.getBinary(0)).thenReturn(
				ExampleTopologyCommonTestUtils.serialize(ExampleTopologyCommonTestUtils.createDataSourcenRecord()));
		when(reader.read(any(), any())).thenReturn(ExampleTopologyCommonTestUtils.createDataSourcenRecord());

		// calling method
		dataBolt.execute(input);

		// Here we just verify a call.
		verify(dataBolt, times(1)).execute(input);
		verify(collector).emit(ExampleTopologyUtils.DATASTREAM, new Values(node));
		verify(collector).ack(input);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testExecuteNull() throws IOException{
		logger.info("Running testExecuteNull...");

		byte[] message = ExampleTopologyCommonTestUtils.serialize(ExampleTopologyCommonTestUtils.createDataSourcenRecord());

		// mocking
		Tuple input = mock(Tuple.class);
		when(input.getBinary(0)).thenReturn(message);
		when(reader.read(any(), any())).thenThrow(IOException.class);

		// calling method
		dataBolt.execute(input);

		// Here we just verify a call.
		verify(dataBolt).execute(input);
		verify(collector).ack(input);
	}

	@Test
	public void testDeclareOutputFields(){
		logger.info("Running testDeclareOutputFields...");

		// mocking
		OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);

		// calling method
		dataBolt.declareOutputFields(declarer);

		// verifying the call
		verify(dataBolt, times(1)).declareOutputFields(declarer);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testDeserialize() throws IOException{
		logger.info("Running testDeserialize...");

		byte[] message = ExampleTopologyCommonTestUtils.serialize(ExampleTopologyCommonTestUtils.createDataSourcenRecord());

		// injecting expected behavior
		when(reader.read(any(), any())).thenThrow(IOException.class);

		// call
		DataNode record = dataBolt.deserialize(message);

		// verify
		assertNull(record);
	}
}
