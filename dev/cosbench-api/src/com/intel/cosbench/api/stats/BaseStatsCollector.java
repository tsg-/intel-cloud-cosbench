package com.intel.cosbench.api.stats;

import com.intel.cosbench.api.context.StatsContext;

/**
 * The stats collector is used to count run-time performance stats.
 * 
 * @author ywang19
 *
 */
public class BaseStatsCollector extends StatsCollector {
	
	private long ts_start;
	private long ts_end;
	
	public BaseStatsCollector() {
		this.ts_start = System.currentTimeMillis();
	}
	
	public BaseStatsCollector(long timestamp) {
		this.ts_start = timestamp;
	}
	
	@Override
	public void onStats(StatsContext context, boolean status) {
		this.ts_end = System.currentTimeMillis();
		System.out.println("Request is " + (status? "succeed" : "failed") + " in " + (ts_end - ts_start) + " milliseconds.");

//		System.out.println("Requesting " + context.getUri() + " is " + (status? "succeed" : "failed") + " with " + context.getLength() + " bytes in " + (ts_end - ts_start) + " milliseconds.");
	}
	   
}
