package com.redislabs.multibench;

/**
 * 
 * @author bob balaban, April 2016
 * 
 * Enumerates the settings known to the benchmark
 * framework. 
 * 
 * Includes for each property a default value. Properties
 * in a property file that are unknown are ignored
 */
public enum BENCHPROPERTIES {
	ITERATIONS("iterations", "10000"),
	REPORTINTERVAL("report", "1000"),
	THREADS("threads", "10"),
	TIMETORUN("timetorun", "30"),
	READSTOWRITES("readstowrites", "2"),
	HOSTPORT("host", "localhost:6379"),
	;

	private String name;
	private String defaultV;
	private BENCHPROPERTIES(String nam, String defV) {
		name = nam;
		defaultV = defV;
	}
	
	public String realName() {
		return this.name;
	}
	public String getDefault() {
		return this.defaultV;
	}
	
} // end enum
