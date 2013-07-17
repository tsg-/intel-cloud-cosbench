package com.intel.cosbench.api.ioengine;

import com.intel.cosbench.api.stats.StatsCollector;
import com.intel.cosbench.api.validator.ResponseValidator;

public interface IOClient {

	public void init(ResponseValidator validator, StatsCollector collector);
}
