package com.coffeetechgaff.storm.enumeration;

import static org.junit.Assert.*;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coffeetechgaff.storm.enumeration.MainVertexName;

/**
 * 
 * @author VivekSubedi
 *
 */
public class MainVertexNameTest{

	private static final Logger logger = LoggerFactory.getLogger(MainVertexNameTest.class);

	@Test
	public void testFromValue(){
		logger.info("Running testFromValue...");
		MainVertexName value = MainVertexName.fromValue("datanode");
		assertEquals(MainVertexName.DATANODE, value);
	}

	@Test
	public void tesWrongValue(){
		logger.info("Running tesWrongValue...");
		MainVertexName value = MainVertexName.fromValue("ALGORITHM");
		assertNotEquals(MainVertexName.ALGORITHM, value);
	}

	@Test
	public void testVertexLabel(){
		logger.info("Running testVertexLabel...");
		MainVertexName value = MainVertexName.fromValue("datanode");
		assertEquals("datanode", value.getVertexLabelName());
	}

	@Test
	public void testEnumNumbers(){
		logger.info("Running testEnumNumbers...");
		MainVertexName[] list = MainVertexName.values();
		assertEquals(2, list.length);
	}

	@Test
	public void testPrintAllEnums(){
		logger.info("Running testPrintAllEnums...");
		MainVertexName[] list = MainVertexName.values();
		StringBuilder builder = new StringBuilder();
		for(MainVertexName myEnum : list){
			builder.append(myEnum.getVertexLabelName());
			builder.append(", ");
		}

		logger.info(builder.substring(0, builder.length() - 2).toString());
	}

}
