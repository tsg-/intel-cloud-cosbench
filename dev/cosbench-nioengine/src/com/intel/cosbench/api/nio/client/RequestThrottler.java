package com.intel.cosbench.api.nio.client;

/**
 * The class encapsulates functionalities to help throttle requests in concurrency.
 * 
 * @author ywang19
 *
 */
public class RequestThrottler {
	private CountUpDownLatchWithLimit latch;
	
	public RequestThrottler(int count) {
		this.latch = new CountUpDownLatchWithLimit(count);
	}
	
	public long countUp() {
		return latch.countUp();
	}
	
	public long countDown() {
		return latch.countDown();
	}
	
	public void await() throws InterruptedException {
		if(latch != null)
			latch.await();
	}
	
	public void dispose() {
		if(latch != null)
			latch.dispose();
	}

}
