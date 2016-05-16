/**
 * 
 */
package com.redislabs.multibench;

import java.util.List;

/**
 * @author bob balaban, April 2016
 * Common data access functions
 *
 */
public interface IConnection {

	public void set(String key, String value) throws Exception;
	public void set(byte [] key, byte [] value) throws Exception;
	
	public void setMany(String [] keys, String [] values) throws Exception;
	public void setMany(List<byte []> keys, List<byte []> values) throws Exception;
	
	public String get(String key) throws Exception;
	public byte [] get(byte [] key) throws Exception;
	
	public void getMany(String [] keys) throws Exception;
	
	public void close();
}
