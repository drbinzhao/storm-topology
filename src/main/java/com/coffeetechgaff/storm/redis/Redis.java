package com.coffeetechgaff.storm.redis;

import redis.clients.jedis.Jedis;

/**
 * Class to that connects to Redis and checks and returns the value of a field.
 * 
 * @author VivekSubedi
 *
 */
public class Redis{

	private IRedisClient redisClient;
	private Jedis jedis;

	public Redis(IRedisClient redis){
		redisClient = redis;
		jedis = redisClient.getResource();
	}

	/**
	 * Returns a boolean true if Redis key and Redis Field exist
	 * 
	 * @param key
	 *            Redis key
	 * @param field
	 *            Redis field
	 * @throws Exception
	 */
	public boolean exists(String key, String field){
		return jedis.hexists(key, field) ? true : false;
	}

	/**
	 * Returns the value of a key from redis
	 * 
	 * @param key
	 *            -key of a property in which the values are stored
	 * @param field
	 *            -field name in which value is stored
	 * @return @String representation value of a field
	 */
	public String getProperty(String key, String field){
		return jedis.hget(key, field);
	}

}
