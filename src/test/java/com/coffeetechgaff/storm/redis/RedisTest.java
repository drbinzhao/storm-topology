package com.coffeetechgaff.storm.redis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coffeetechgaff.storm.redis.IRedisClient;
import com.coffeetechgaff.storm.redis.Redis;

import redis.clients.jedis.Jedis;

/**
 * 
 * @author VivekSubedi
 *
 */
public class RedisTest{

	private static final Logger logger = LoggerFactory.getLogger(RedisTest.class);

	@Test
	public void testExists(){
		logger.info("Running testExists...");
		String key = "directory";
		String field = "datasource.topic";

		IRedisClient redisClient = mock(IRedisClient.class);
		Jedis jedis = mock(Jedis.class);
		when(redisClient.getResource()).thenReturn(jedis);
		Redis redis = new Redis(redisClient);
		when(jedis.hexists(key, field)).thenReturn(true);

		boolean exists = redis.exists(key, field);
		assertTrue(exists);

		when(jedis.hexists(key, field)).thenReturn(false);

		boolean exists2 = redis.exists(key, field);
		assertFalse(exists2);
	}

	@Test
	public void testGetProperty(){
		logger.info("Running testGetProperty...");
		String key = "directory";
		String field = "datasource.topic";

		IRedisClient redisClient = mock(IRedisClient.class);
		Jedis jedis = mock(Jedis.class);
		when(redisClient.getResource()).thenReturn(jedis);
		Redis redis = new Redis(redisClient);
		when(jedis.hget(key, field)).thenReturn("datasources");

		String property = redis.getProperty(key, field);
		assertEquals("datasources", property);
	}

}
