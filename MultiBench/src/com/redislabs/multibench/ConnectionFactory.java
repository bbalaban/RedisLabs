/**
 * 
 */
package com.redislabs.multibench;

import java.util.*;

import redis.clients.jedis.*;

/**
 * @author bob balaban, April 2016
 *
 * Factory class that delivers Connection
 * instances of various types
 */

public class ConnectionFactory {
	
	// the types of connections we know about
	public static enum CONNECTION_TYPE {
		REDIS_SIMPLE, 
		REDIS_CLUSTER,
		REDIS_SENTINEL,
		RLEC,
	};
	
	private CONNECTION_TYPE type = null;
	private HostAndPort singleHost = null;
	private Set<HostAndPort> multiHost = null;
	private JedisPool jPool = null;
	boolean initialized = false;

	
	public ConnectionFactory() {}
	public void initSingle(String hostPort, CONNECTION_TYPE t) {
		if (this.multiHost != null) {
			throw new IllegalStateException("Multi-host already initialized");
		}
		if (t != CONNECTION_TYPE.REDIS_SIMPLE  &&
			t != CONNECTION_TYPE.RLEC) {
			throw new IllegalArgumentException("Connection type " + t.name() + 
						" is not a single-host type");
		}
		if (this.initialized) {
			throw new IllegalStateException("Already initialized");
		}
		this.singleHost = toHaP(hostPort);
		this.type = t;
		
		// init JedisPool for both types of single host connections
		int timeout = 10000; // 10 sec
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(100);
		this.jPool = new JedisPool(config, this.singleHost.getHost(), this.singleHost.getPort(), timeout);
		this.initialized = true;
	}
	
	public void initMulti(Set<String> hosts, CONNECTION_TYPE t) {
		if (this.singleHost != null) {
			throw new IllegalStateException("Single host already initialized");
		}
		if (t != CONNECTION_TYPE.REDIS_CLUSTER && 
			t != CONNECTION_TYPE.REDIS_SENTINEL) {
			throw new IllegalArgumentException("Connection type " + t.name() + 
					" is not a multi-host type");
		}
		this.multiHost = new HashSet<HostAndPort> (hosts.size());
		for (String h : hosts) {
			HostAndPort hap = toHaP(h);
			this.multiHost.add(hap);
		}
		this.type = t;
		
		//TODO: Init sentinel pool or cluster
		this.initialized = true;
	}
	
	public IConnection getConnection() {
		switch (this.type) {
			case REDIS_SIMPLE:
				return new RedisConnection(this.jPool);
				
			case RLEC:
				return new RedisConnection(this.jPool);
				
			case REDIS_CLUSTER:
				//TODO: return cluster wrapper
				return null;
				
			case REDIS_SENTINEL:
				//TODO: Return Jedis from SentinelPool
				return null;
		} // end switch
		
		return null;
	}
	
	public void close() {
		if (this.jPool != null) {
			this.jPool.close();
		}
	}
	
	
	private HostAndPort toHaP(String s) {
		String [] pieces = s.split(":");
		int port = Integer.parseInt(pieces[1]);
		return new HostAndPort(pieces[0], port);
	}
} // end class
