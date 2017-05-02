package com.coffeetechgaff.storm.bolt;

import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
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

import com.coffeetechgaff.storm.algorithmnode.AlgorithmNode;
import com.coffeetechgaff.storm.bolt.AlgorithmBolt;
import com.coffeetechgaff.storm.utils.ExampleTopologyCommonTestUtils;
import com.coffeetechgaff.storm.utils.ExampleTopologyUtils;

/**
 * 
 * @author VivekSubedi
 *
 */
public class AlgorithmBoltTest{

	private static final Logger logger = LoggerFactory.getLogger(AlgorithmBoltTest.class);

	@InjectMocks
	@Spy
	private AlgorithmBolt algorithmBolt;

	@Mock
	private DatumReader<AlgorithmNode> reader;

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
		algorithmBolt.prepare(map, context, collector);

		// verifying the call
		verify(algorithmBolt, times(1)).prepare(map, context, collector);
	}

	@Test
	public void testExecute() throws IOException{
		logger.info("Running testExecute...");

		// mocking
		Tuple input = mock(Tuple.class);
		when(input.getBinary(0)).thenReturn(getByteArray());
		when(reader.read(any(), any())).thenReturn(ExampleTopologyCommonTestUtils.getAnalyticDefinationObject());

		// calling method
		algorithmBolt.execute(input);

		// Here we just verify a call.
		verify(algorithmBolt, times(1)).execute(input);
		verify(collector).emit(ExampleTopologyUtils.ALGORITHMSTREAM,
				new Values(ExampleTopologyCommonTestUtils.getAnalyticDefinationObject()));
		verify(collector).ack(input);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testExecuteNull() throws IOException{
		logger.info("Running testExecuteNull...");

		// mocking
		Tuple input = mock(Tuple.class);
		when(input.getBinary(0)).thenReturn(getByteArray());
		when(reader.read(any(), any())).thenThrow(IOException.class);

		// calling method
		algorithmBolt.execute(input);

		// Here we just verify a call.
		verify(algorithmBolt, times(1)).execute(input);
		verify(collector).ack(input);
	}

	@Test
	public void testDeclareOutputFields(){
		logger.info("Running testDeclareOutputFields...");

		// mocking
		OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);

		// calling method
		algorithmBolt.declareOutputFields(declarer);

		// verifying the call
		verify(algorithmBolt, times(1)).declareOutputFields(declarer);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testDeserialize() throws IOException{
		logger.info("Running testDeserialize...");

		byte[] message = getByteArray();

		// injecting expected behavior
		when(reader.read(any(), any())).thenThrow(IOException.class);

		// call
		AlgorithmNode record = algorithmBolt.deserialize(message);

		// verify
		assertNull(record);
	}

	private byte[] getByteArray() throws IOException{
		AlgorithmNode ad = ExampleTopologyCommonTestUtils.getAnalyticDefinationObject();

		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = null;
		out = new ObjectOutputStream(bos);
		out.writeObject(ad.toString());
		out.flush();
		byte[] myBytes = bos.toByteArray();
		bos.close();
		return myBytes;
	}

}
