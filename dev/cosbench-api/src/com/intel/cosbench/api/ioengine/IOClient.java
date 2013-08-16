package com.intel.cosbench.api.ioengine;

import com.intel.cosbench.api.stats.StatsListener;
import com.intel.cosbench.api.validator.ResponseValidator;

public interface IOClient {

	public void setValidator(ResponseValidator validator);	
	public void setListener(StatsListener listener);
}
