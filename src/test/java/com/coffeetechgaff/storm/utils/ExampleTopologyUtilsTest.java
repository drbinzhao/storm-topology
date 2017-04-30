package com.coffeetechgaff.storm.utils;

import static org.junit.Assert.*;

import java.lang.reflect.Constructor;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coffeetechgaff.storm.utils.ExampleTopologyUtils;

public class ExampleTopologyUtilsTest{

	private static final Logger logger = LoggerFactory.getLogger(ExampleTopologyUtilsTest.class);

	@Test
	public void evilConstructorInaccessibilityTest() throws Exception{
		logger.info("Running evilConstructorInaccessibilityTest...");
		Constructor<?>[] ctors = ExampleTopologyUtils.class.getDeclaredConstructors();
		assertEquals("Utility class should only have one constructor", 1, ctors.length);
		Constructor<?> ctor = ctors[0];
		assertFalse("Utility class constructor should be inaccessible", ctor.isAccessible());
		ctor.setAccessible(true); // obviously we'd never do this in production
		assertEquals("You'd expect the construct to return the expected type", ExampleTopologyUtils.class, ctor.newInstance()
				.getClass());
	}

}
