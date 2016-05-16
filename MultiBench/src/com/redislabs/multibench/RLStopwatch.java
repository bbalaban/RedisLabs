package com.redislabs.multibench;

/**
 * Elapsed time stopwatch. Must be started and stopped on the thread where it is
 * created, but can be queried for elapsed time from any thread. Cannot be
 * started twice. Times are in milliseconds
 */
public class RLStopwatch {

	private volatile long startTime = 0;
	private long stopTime = 0;
	private volatile boolean stopped = false;

	public RLStopwatch() {
	}

	public void start() {
		if (this.startTime > 0) {
			throw new IllegalStateException("Cannot start twice");
		}
		this.startTime = System.currentTimeMillis();
	}
	
	public synchronized void stop() {
		if (this.stopped) {
			throw new IllegalStateException("Cannot stop twice");
		}
		if (this.startTime == 0) {
			throw new IllegalStateException("Never started!");
		}
		this.stopTime = System.currentTimeMillis();
		this.stopped = true;
	}

	public long elapsed() {
		if (!this.stopped) {
				return System.currentTimeMillis() - this.startTime; 
		} else {
			return this.stopTime - this.startTime;
		}
	}
} // end RLStopwatch
