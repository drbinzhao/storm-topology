package com.coffeetechgaff.storm.validation;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coffeetechgaff.storm.algorithmnode.AlgorithmNode;
import com.coffeetechgaff.storm.utils.ExampleTopologyCommonTestUtils;
import com.coffeetechgaff.storm.validation.AlgorithmValidation;

public class AlgorithmValidationTest{

	private static final Logger logger = LoggerFactory.getLogger(AlgorithmValidationTest.class);

	private static AlgorithmValidation algorithmValidation;
	private AlgorithmNode ad;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception{
		algorithmValidation = new AlgorithmValidation();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception{
		algorithmValidation = null;
	}

	@Test
	public void validateAnalyticTest(){
		logger.info("Start validateAnalyticTest");
		ad = ExampleTopologyCommonTestUtils.getAlgorithmObject();
		ad.setUid(null);
		logger.info("test AlgorithmNode uid");
		Map<String, String> ad1 = algorithmValidation.validateAnalytic(ad);
		assertFalse(ad1.containsKey("id"));

		logger.info("test AlgorithmNode name");
		ad.setName(null);
		Map<String, String> ad2 = algorithmValidation.validateAnalytic(ad);
		assertFalse(ad2.containsKey("name"));

		logger.info("test AlgorithmNode description");
		ad.setDescription(null);
		Map<String, String> ad3 = algorithmValidation.validateAnalytic(ad);
		assertFalse(ad3.containsKey("description"));

		logger.info("test AlgorithmNode author");
		ad.setAuthor(null);
		Map<String, String> ad4 = algorithmValidation.validateAnalytic(ad);
		assertFalse(ad4.containsKey("author"));

		logger.info("test AlgorithmNode email");
		ad.setEmail(null);
		Map<String, String> ad5 = algorithmValidation.validateAnalytic(ad);
		assertFalse(ad5.containsKey("email"));

		logger.info("test AlgorithmNode version");
		ad.setVersion(null);
		Map<String, String> ad6 = algorithmValidation.validateAnalytic(ad);
		assertFalse(ad6.containsKey("version"));

		logger.info("test AlgorithmNode input");
		ad.getInput().get(0).setName(null);
		ad.getInput().get(0).setType(null);
		Map<String, String> ad7 = algorithmValidation.validateAnalytic(ad);
		assertTrue(ad7.containsKey("input"));
		assertTrue(ad7.get("input").toString().contains("example2"));

		logger.info("test AlgorithmNode input again");
		ad.getInput().get(1).setName(null);
		ad.getInput().get(1).setType(null);
		Map<String, String> ad8 = algorithmValidation.validateAnalytic(ad);
		assertFalse(ad8.containsKey("input"));

		logger.info("test AlgorithmNode output");
		ad.getOutput().get(0).setName(null);
		ad.getOutput().get(0).setType(null);
		Map<String, String> ad9 = algorithmValidation.validateAnalytic(ad);
		assertFalse(ad9.containsKey("output"));

		logger.info("test AlgorithmNode Parameter");
		AlgorithmNode ad10 = ad;
		ad.getParameters().get(1).setDefaultValue(null);
		ad.getParameters().get(1).setName(null);
		ad.getParameters().get(1).setDescription(null);
		ad.getParameters().get(1).setUiHint(null);
		ad.getParameters().get(1).setType(null);
		ad.getParameters().get(1).setEntity(null);
		Map<String, String> ad11 = algorithmValidation.validateAnalytic(ad);
		assertTrue(ad11.containsKey("parameters"));
		assertTrue(ad11.get("parameters").toString().contains("counting analytics"));

		logger.info("test AlgorithmNode Parameter again");
		ad10.getParameters().get(0).setDefaultValue(null);
		ad10.getParameters().get(0).setName(null);
		ad10.getParameters().get(0).setDescription(null);
		ad10.getParameters().get(0).setUiHint(null);
		ad10.getParameters().get(0).setType(null);
		ad10.getParameters().get(0).setEntity(null);
		Map<String, String> ad12 = algorithmValidation.validateAnalytic(ad);
		assertFalse(ad12.containsKey("parameters"));
	}

	@Test
	public void testEmptyInputOutputParameters(){
		logger.info("Running testEmptyInputOutputParameters...");

		AlgorithmNode ad = ExampleTopologyCommonTestUtils.getAlgorithmObject();

		logger.info("test AlgorithmNode input with empty array");
		AlgorithmNode ad8 = ad;
		ad8.setInput(new ArrayList<>());
		Map<String, String> ad1 = algorithmValidation.validateAnalytic(ad);
		assertFalse(ad1.containsKey("input"));

		logger.info("test AlgorithmNode input with null");
		ad8.setInput(null);
		Map<String, String> ad2 = algorithmValidation.validateAnalytic(ad);
		assertFalse(ad2.containsKey("input"));

		logger.info("test AlgorithmNode output with empty array");
		AlgorithmNode ad9 = ad;
		ad9.setOutput(new ArrayList<>());
		Map<String, String> ad3 = algorithmValidation.validateAnalytic(ad);
		assertFalse(ad3.containsKey("output"));

		logger.info("test AlgorithmNode output with null");
		ad9.setOutput(null);
		Map<String, String> ad4 = algorithmValidation.validateAnalytic(ad);
		assertFalse(ad4.containsKey("output"));

		logger.info("test AlgorithmNode Parameter with empty array");
		AlgorithmNode ad10 = ad;
		ad10.setParameters(new ArrayList<>());
		Map<String, String> ad5 = algorithmValidation.validateAnalytic(ad);
		assertFalse(ad5.containsKey("parameters"));

		logger.info("test AlgorithmNode Parameter with null");
		ad10.setParameters(null);
		Map<String, String> ad6 = algorithmValidation.validateAnalytic(ad);
		assertFalse(ad6.containsKey("parameters"));
	}

}
