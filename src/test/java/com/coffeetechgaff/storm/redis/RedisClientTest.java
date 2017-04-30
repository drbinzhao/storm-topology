package com.coffeetechgaff.storm.redis;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import com.coffeetechgaff.storm.redis.IRedisClient;
import com.fiftyonred.mock_jedis.MockJedis;

import redis.clients.jedis.Jedis;

public class RedisClientTest implements IRedisClient{
	private Jedis client = null;

	public RedisClientTest(){
		client = new MockJedis("localhost");
	}

	@Override
	public Jedis getResource(){
		if(client == null){
			client = new MockJedis("localhost");
		}
		return client;
	}

	@Override
	public void close() throws IOException{
		client.close();
	}

	@Override
	public void returnResource(Jedis jedis){
		jedis.close();
	}

	@Test
	public void test(){
		Jedis j = new MockJedis("test");
		j.set("here", "123");
		assertEquals("123", j.get("here"));
		j.close();
	}
}
