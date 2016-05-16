package com.redislabs.multibench;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import com.redislabs.multibench.ConnectionFactory.CONNECTION_TYPE;

/**
 * 
 * @author bob balaban, April 2016
 *
 *         Run a benchmark for simple set/get commands
 */
public class RedisSetGetBench {

	/**
	 * 
	 * @param args
	 *            <property file path>
	 */
	public static void main(String[] args) {

		if (args == null || args.length == 0) {
			printUsage();
			return;
		}

		try {
			FileInputStream fis = new FileInputStream(args[0]);
			Properties prop = new Properties();
			prop.load(fis);
			fis.close();

			// get the settings we care about
			String szThreads = prop.getProperty(BENCHPROPERTIES.THREADS.realName(),
					BENCHPROPERTIES.THREADS.getDefault());
			int nThreads = Integer.parseInt(szThreads);
			// String szIterations =
			// prop.getProperty(BENCHPROPERTIES.ITERATIONS.realName(),
			// BENCHPROPERTIES.ITERATIONS.getDefault());
			// long nIterations = Long.parseLong(szIterations);
			String szReportInterval = prop.getProperty(BENCHPROPERTIES.REPORTINTERVAL.realName(),
					BENCHPROPERTIES.REPORTINTERVAL.getDefault());
			int nReportinterval = Integer.parseInt(szReportInterval);
			String szTimeToRun = prop.getProperty(BENCHPROPERTIES.TIMETORUN.realName(),
					BENCHPROPERTIES.TIMETORUN.getDefault());
			int nTimeToRun = Integer.parseInt(szTimeToRun);
			String host = prop.getProperty(BENCHPROPERTIES.HOSTPORT.realName(), BENCHPROPERTIES.HOSTPORT.getDefault());
			String szReadsWrites = prop.getProperty(BENCHPROPERTIES.READSTOWRITES.realName(), BENCHPROPERTIES.READSTOWRITES.getDefault());
			int readToWrite = Integer.parseInt(szReadsWrites);

			// ignoring iterations and just using time-to-run
			System.out.println("Redis get/set benchmark: " + nThreads + " threads, " + nTimeToRun + " seconds runtime");
			System.out.println("Host = " + host + ", read-to-write ratio = " + readToWrite);

			// create the shared counter and stopwatch, and the connection
			// factory
			AtomicLong counter = new AtomicLong();
			RLStopwatch stopwatch = new RLStopwatch();
			ConnectionFactory cxnFactory = new ConnectionFactory();
			cxnFactory.initSingle(host, CONNECTION_TYPE.RLEC);

			// init the reporting thread
			ReportThread reportThread = new ReportThread(counter, stopwatch, nReportinterval);
			reportThread.start();

			// create all the worker threads
			TestWorker[] wThreads = new TestWorker[nThreads];
			stopwatch.start();
			for (int i = 0; i < nThreads; i++) {
				IConnection cxn = cxnFactory.getConnection();
				wThreads[i] = new TestWorker(i + 1, counter, stopwatch, nTimeToRun, readToWrite, cxn);
				wThreads[i].start();
			}

			// wait until all are done
			for (int i = 0; i < nThreads; i++) {
				try {
					wThreads[i].join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			// kill the report thread
			reportThread.stopRunning();

			// stop the timer
			stopwatch.stop();

			cxnFactory.close();
			System.out.println("All done, exiting");

		} catch (IOException e) {
			printUsage();
			e.printStackTrace();
		}
	}

	public static void printUsage() {
		System.out.println("Usage: java -jar RedisSetGetBench.jar <property file path>");
	}

	// worker thread to actually execute the test
	static class TestWorker extends Thread {
		private AtomicLong counter;
		private RLStopwatch stopwatch;
		private int timeToRun;
		private IConnection conn;
		private int index;
		private int readToWrite;

		public TestWorker(int idx, AtomicLong count, RLStopwatch sw, int time, int rtw, IConnection cxn) {
			index = idx;
			counter = count;
			stopwatch = sw;
			timeToRun = time;
			conn = cxn;
			readToWrite = rtw;
		}

		@Override
		public void run() {
			System.out.println("Starting thread " + this.index);

			try {
				long starttimeSec = this.stopwatch.elapsed() / 1000;
				int loops = 0;
				int nBatch = 50;
				int i;
				String[] keys = new String[nBatch];
				while (true) {
					for (i = 0; i < nBatch; i++) {
						keys[i] = "Thread" + this.index + ":" + ++loops;
					}
					this.conn.setMany(keys, keys);
					this.counter.addAndGet(nBatch);

					for (i = 0; i < this.readToWrite; i++) {
						this.conn.getMany(keys);
						this.counter.addAndGet(nBatch);
					}

					long now = this.stopwatch.elapsed() / 1000;
					if ((int) (now - starttimeSec) > this.timeToRun) {
						break;
					}
				} // end while
			} catch (Exception e) {
				System.err.println("Exception in thread " + this.index);
				e.printStackTrace();
			} finally {
				this.conn.close();
				System.out.println("Ending thread " + this.index);
			}
		}
	} // end TestWorker

} // end main class
