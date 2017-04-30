package com.coffeetechgaff.storm.redis;

import java.io.Closeable;
import java.io.IOException;

import redis.clients.jedis.Jedis;

public interface IRedisClient extends Closeable{

	public Jedis getResource();

	public void returnResource(Jedis jedis);

	@Override
	public void close() throws IOException;
}
