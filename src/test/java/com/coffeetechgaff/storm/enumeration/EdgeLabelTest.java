package com.coffeetechgaff.storm.enumeration;

import static org.junit.Assert.*;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coffeetechgaff.storm.enumeration.EdgeLabelEnums;

/**
 * 
 * @author VivekSubedi
 *
 */
public class EdgeLabelTest{

	private static final Logger logger = LoggerFactory.getLogger(EdgeLabelTest.class);

	@Test
	public void testFromValue(){
		logger.info("Running testFromValue...");
		EdgeLabelEnums value = EdgeLabelEnums.fromValue("works with");
		assertEquals(EdgeLabelEnums.WORKSWITH, value);
	}

	@Test
	public void tesWrongValue(){
		logger.info("Running tesWrongValue...");
		EdgeLabelEnums value = EdgeLabelEnums.fromValue("WORKS WITH");
		assertNotEquals(EdgeLabelEnums.WORKSWITH, value);
	}

	@Test
	public void testVertexLabel(){
		logger.info("Running testVertexLabel...");
		EdgeLabelEnums value = EdgeLabelEnums.fromValue("works with");
		assertEquals("works with", value.getEdgeLabelName());
	}

	@Test
	public void testEnumNumbers(){
		logger.info("Running testEnumNumbers...");
		EdgeLabelEnums[] list = EdgeLabelEnums.values();
		assertEquals(2, list.length);
	}

	@Test
	public void testPrintAllEnums(){
		logger.info("Running testPrintAllEnums...");
		EdgeLabelEnums[] list = EdgeLabelEnums.values();
		StringBuilder builder = new StringBuilder();
		for(EdgeLabelEnums myEnum : list){
			builder.append(myEnum.getEdgeLabelName());
			builder.append(", ");
		}

		logger.info(builder.substring(0, builder.length() - 2).toString());
	}
}
