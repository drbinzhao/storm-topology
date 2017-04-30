package com.coffeetechgaff.storm.validation;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coffeetechgaff.storm.datanode.DataNode;
import com.coffeetechgaff.storm.datanode.Operation;
import com.coffeetechgaff.storm.validation.DataValidation;

public class DataNodeValidationTest{

	private static Logger logger = LoggerFactory.getLogger(DataNodeValidationTest.class);
	private static DataValidation validation;

	@BeforeClass
	public static void setUpBeforeClass(){
		validation = new DataValidation();
	}

	@AfterClass
	public static void tearDownAfterClass(){
		validation = null;
	}

	@Test
	public void testNullId(){
		DataNode record = new DataNode(Operation.CREATE, null, createSampleList(), "FCMS-Gold",
				"The XZ-1 source.", "Unclassified", "Gold");

		logger.info("Running testNullId() ...");
		logger.debug("Before validation: ");
		describe(record);
		assertNull(record.getId());
		Map<String, String> validatedMap = validation.validateDatasource(record);
		assertFalse(validatedMap.containsKey("id"));
	}

	@Test
	public void testNullDataTypes(){
		DataNode record = new DataNode(Operation.CREATE, "101", null, "FCMS-Gold",
				"The XZ-1 source.", "Unclassified", "Gold");

		logger.info("Running testNullDataTypes() ...");

		logger.debug("Before validation: ");
		describe(record);

		assertNull(record.getDataTypes());

		Map<String, String> validatedMap = validation.validateDatasource(record);
		assertFalse(validatedMap.containsKey("dataTypes"));
	}

	@Test
	public void testNullName(){
		DataNode record = new DataNode(Operation.CREATE, "101", createSampleList(), null,
				"The XZ-1 source.", "Unclassified", "Gold");

		logger.info("Running testNullName() ...");

		logger.debug("Before validation: ");
		describe(record);

		assertNull(record.getName());

		Map<String, String> validatedMap = validation.validateDatasource(record);
		assertFalse(validatedMap.containsKey("name"));
	}

	@Test
	public void testNullDescription(){
		DataNode record = new DataNode(Operation.CREATE, "101", createSampleList(),
				"FCMS-Gold", null, "Unclassified", "Gold");

		logger.info("Running testNullDescription() ...");

		logger.debug("Before validation: ");
		describe(record);

		assertNull(record.getDescription());

		Map<String, String> validatedMap = validation.validateDatasource(record);
		assertFalse(validatedMap.containsKey("description"));
	}

	@Test
	public void testNullClassification(){
		DataNode record = new DataNode(Operation.CREATE, "101", createSampleList(),
				"FCMS-Gold", "The XZ-1 source.", null, "Gold");

		logger.info("Running testNullClassification() ...");

		logger.debug("Before validation: ");
		describe(record);

		assertNull(record.getClassification());

		Map<String, String> validatedMap = validation.validateDatasource(record);
		assertFalse(validatedMap.containsKey("classification"));
	}

	@Test
	public void testNullMaturity(){
		DataNode record = new DataNode(Operation.CREATE, "101", createSampleList(),
				"FCMS-Gold", "The XZ-1 source.", "Unclassified", null);

		logger.info("Running testNullMaturity() ...");

		logger.debug("Before validation: ");
		describe(record);

		assertNull(record.getMaturity());

		Map<String, String> validatedMap = validation.validateDatasource(record);
		assertFalse(validatedMap.containsKey("maturity"));
	}

	private List<String> createSampleList(){
		return Arrays.asList("kafka", "WFS", "CSV");
	}

	private void describe(DataNode record){
		// print out the fields of this record

		logger.debug("id: {}", fixNull(record.getId()));

		List<String> dataTypes = record.getDataTypes();
		if(dataTypes != null){
			if(dataTypes.size() > 0){
				StringBuffer flatList = new StringBuffer();

				for(String dataType : dataTypes){
					flatList.append(dataType);
					flatList.append(",");
				}

				flatList.deleteCharAt(flatList.length() - 1);

				logger.debug("dataTypes: {}", flatList.toString());
			}else{
				logger.debug("dataTypes: <empty>");
			}
		}else{
			logger.info("dataTypes: <null>");
		}

		logger.debug("name: {}", fixNull(record.getName()));
		logger.debug("description: {}", fixNull(record.getDescription()));
		logger.debug("classification: {}", fixNull(record.getClassification()));
		logger.debug("maturity: {}", fixNull(record.getMaturity()));
	}

	private String fixNull(String s){
		if(s == null){
			return "<null>";
		}

		return s;
	}
}
