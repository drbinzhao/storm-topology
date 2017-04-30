package com.coffeetechgaff.storm.enumeration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coffeetechgaff.storm.enumeration.CommonVertexLabelEnums;

/**
 * 
 * @author VivekSubedi
 *
 */
public class CommonVertexLabelEnumsTest{

	private static final Logger logger = LoggerFactory.getLogger(CommonVertexLabelEnumsTest.class);

	@Test
	public void testFromValue(){
		logger.info("Running testFromValue...");
		CommonVertexLabelEnums value = CommonVertexLabelEnums.fromValue("id");
		assertEquals(CommonVertexLabelEnums.ID, value);
	}

	@Test
	public void tesWrongValue(){
		logger.info("Running tesWrongValue...");
		CommonVertexLabelEnums value = CommonVertexLabelEnums.fromValue("ID");
		assertNotEquals(CommonVertexLabelEnums.ID, value);
	}

	@Test
	public void testVertexLabel(){
		logger.info("Running testVertexLabel...");
		CommonVertexLabelEnums value = CommonVertexLabelEnums.fromValue("dataTypes");
		assertEquals("dataTypes", value.getVertexLabelName());
	}

	@Test
	public void testEnumNumbers(){
		logger.info("Running testEnumNumbers...");
		CommonVertexLabelEnums[] list = CommonVertexLabelEnums.values();
		assertEquals(13, list.length);
	}

	@Test
	public void testPrintAllEnums(){
		logger.info("Running testPrintAllEnums...");
		CommonVertexLabelEnums[] list = CommonVertexLabelEnums.values();
		StringBuilder builder = new StringBuilder();
		for(CommonVertexLabelEnums myEnum : list){
			builder.append(myEnum.getVertexLabelName());
			builder.append(", ");
		}

		logger.info(builder.substring(0, builder.length() - 2).toString());
	}
}
