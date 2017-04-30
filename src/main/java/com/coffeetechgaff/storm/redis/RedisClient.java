package com.coffeetechgaff.storm.redis;

import java.io.IOException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisClient implements IRedisClient{
	private static final String HOST = "192.168.99.100"; //can be change depending on the user
	private static final int PORT = 9023; // port will change as well
	private JedisPool pool;

	public RedisClient(){
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(200);
		pool = new JedisPool(config, HOST, PORT);
	}

	public RedisClient(String host){
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(200);
		pool = new JedisPool(config, host);
	}

	public RedisClient(String host, int port){
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(200);
		pool = new JedisPool(config, host, port);
	}

	@Override
	public Jedis getResource(){
		Jedis jedis = this.pool.getResource();
		return jedis;
	}

	@Override
	public void returnResource(Jedis jedis){
		jedis.close();
	}

	@Override
	public void close() throws IOException{
		this.pool.destroy();
	}
}
