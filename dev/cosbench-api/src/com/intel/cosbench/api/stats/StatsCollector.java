package com.intel.cosbench.api.stats;

import com.intel.cosbench.api.context.StatsContext;

public abstract class StatsCollector {

	public abstract void onStats(StatsContext context, boolean status); 
	
}
