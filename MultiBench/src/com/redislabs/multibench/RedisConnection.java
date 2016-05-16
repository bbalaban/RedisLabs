/**
 * 
 */
package com.redislabs.multibench;

import java.util.List;

import redis.clients.jedis.*;

/**
 * @author bob balaban, April 2016 Simple wrapper for JedisConnection
 * 
 *         NOT THREAD SAFE
 */
public class RedisConnection implements IConnection {

	private Jedis jedis = null;
	private JedisPool pool = null;

	public RedisConnection(JedisPool p) {
		this.pool = p;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.redislabs.multibench.IConnection#set(java.lang.String,
	 * java.lang.String)
	 */
	@Override
	public void set(String key, String value) throws Exception {
		assureConnection();
		this.jedis.set(key, value);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.redislabs.multibench.IConnection#set(byte[], byte[])
	 */
	@Override
	public void set(byte[] key, byte[] value) throws Exception {
		assureConnection();
		this.jedis.set(key, value);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.redislabs.multibench.IConnection#get(java.lang.String)
	 */
	@Override
	public String get(String key) throws Exception {
		assureConnection();
		return this.jedis.get(key);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.redislabs.multibench.IConnection#get(byte[])
	 */
	@Override
	public byte[] get(byte[] key) throws Exception {
		assureConnection();
		return this.jedis.get(key);
	}
	
	@Override
	public void getMany(String[] keys) throws Exception {
		assureConnection();
		Pipeline pipe = this.jedis.pipelined();
		for (int i = 0; i < keys.length; i++) {
			pipe.get(keys[i]);
		}
		pipe.sync();
		pipe.close();
	}

	@Override
	public void setMany(String [] keys, String [] values) throws Exception {
		if (keys.length != values.length) {
			throw new IllegalArgumentException("List sizes must match");
		}
		
		assureConnection();
		Pipeline pipe = this.jedis.pipelined();
		for (int i = 0; i < keys.length; i++) {
			pipe.set(keys[i], values[i]);
		}
		pipe.sync();
		pipe.close();
	}

	@Override
	public void setMany(List<byte[]> keys, List<byte[]> values) throws Exception {
		if (keys.size() != values.size()) {
			throw new IllegalArgumentException("List sizes must match");
		}
		
		assureConnection();
		Pipeline pipe = this.jedis.pipelined();
		for (int i = 0; i < keys.size(); i++) {
			pipe.set(keys.get(i), values.get(i));
		}
		pipe.sync();
		pipe.close();
	}

	@Override
	public void close() {
		if (this.jedis != null) {
			this.jedis.close();
			this.jedis = null;
		}
	} // end close

	private void assureConnection() {
		if (this.pool == null) {
			throw new IllegalStateException("No pool!");
		}
		if (this.jedis == null) {
			this.jedis = this.pool.getResource();
		}
	}


} // end class
