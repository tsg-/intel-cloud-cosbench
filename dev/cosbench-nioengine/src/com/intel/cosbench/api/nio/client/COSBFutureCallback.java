package com.intel.cosbench.api.nio.client;

import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.util.Asserts;

import com.intel.cosbench.api.context.ExecContext;
import com.intel.cosbench.api.stats.StatsCollector;
import com.intel.cosbench.api.validator.ResponseValidator;


/**
 * The future callback class especially for COSBench use.
 *    
 * @author ywang19
 *
 */
public class COSBFutureCallback implements FutureCallback<HttpResponse> {
	private RequestThrottler throttler;	// the throttler is to throttle request rate.
	
//	private HttpHost target;
	private ExecContext context;  // data exchange for async response handling.	

	private ResponseValidator validator;
	private StatsCollector collector;
	
	public COSBFutureCallback(final RequestThrottler throttler) {
		Asserts.notNull(throttler, "Request Throttler shouldn't be null.");
		
		this.throttler = throttler;
		this.throttler.countUp();
	}
	
	public COSBFutureCallback(final RequestThrottler throttler, ExecContext context) {		
		this(throttler);
		this.context = context;
	}
	
	public COSBFutureCallback(final RequestThrottler throttler, ExecContext context, ResponseValidator validator) {		
		this(throttler, context);
		this.validator = validator;
	}
	
	public COSBFutureCallback(final RequestThrottler throttler, ExecContext context, ResponseValidator validator, StatsCollector collector) {		
		this(throttler, context, validator);
		this.collector = collector;		
	}
	
	public void setValidator(ResponseValidator validator) {
		this.validator = validator;
	}
	
	public void setCollector(StatsCollector collector) {
		this.collector = collector;
	}
	
//	public long countUp() {
//		return throttler.countUp();
//	}
	
	public long countDown() {
		return throttler.countDown();
	}
	
//	public void await() throws InterruptedException {
//		if(throttler != null)
//			throttler.await();
//	}

	@Override
    public void completed(final HttpResponse response) {    
		context.response = response;
		
        System.out.println("COMPLETED: " + context.getUri() + "->" + response.getStatusLine() + "\t Outstanding Request is " + countDown());
    	
        boolean status = false;
    	try {
    		status = validator.validate(response, context);
    	}catch(Throwable e) {
    		System.out.println("Response can't pass validation with exception: " + e.getMessage());
    		e.printStackTrace();
    	}finally {
    		collector.onStats(context, status);    	
    	}
    }

	@Override
    public void failed(final Exception ex) {
        System.out.println("FAILED: " + context.getUri() + "->" + ex.getMessage() + "\t Outstanding Request is " + countDown());
//        ex.printStackTrace();
   		collector.onStats(context, false);  
    }

	@Override
    public void cancelled() {
        System.out.println("CANCELLED: " + context.getUri() + " cancelled" + "\t Outstanding Request is " + countDown());
    }
    
}
