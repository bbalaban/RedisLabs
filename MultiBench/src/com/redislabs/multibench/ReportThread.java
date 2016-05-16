/**
 * 
 */
package com.redislabs.multibench;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author bob balaban, April 2016
 *
 *         Write a couple of metrics every so often based on an accumulator and
 *         a stopwatch
 */
public class ReportThread extends Thread {

	private RLStopwatch stopwatch = null;
	private AtomicLong counter = null;
	private int interval;
	private boolean stop = false;

	@SuppressWarnings("unused")
	private ReportThread() {
	}

	public ReportThread(AtomicLong al, RLStopwatch sw, int i) {
		interval = i;
		stopwatch = sw;
		counter = al;
	}

	public void stopRunning() {
		this.stop = true;
	}

	@Override
	public void run() {
		long elapsedMS;
		int elapsedSec;
		long lastC = 0;

		while (!stop) {
			long c = this.counter.get();
			if ((c > 0) && (c != lastC) && (c % this.interval == 0)) {
				lastC = c;
				elapsedMS = this.stopwatch.elapsed();
				elapsedSec = (int) (elapsedMS / 1000);
				if (elapsedSec > 0) {
					int opsPerSec = (int) (c / elapsedSec);
					double MsPerOp = (double) elapsedMS / (double) c;
					System.out.println(c + " operations: " + opsPerSec + " ops/sec, " + MsPerOp + " millisec latency");
					try {
						Thread.sleep((long)1);
					} catch (InterruptedException e) {	}
				}
			}
		} // end while
	}

} // end class
