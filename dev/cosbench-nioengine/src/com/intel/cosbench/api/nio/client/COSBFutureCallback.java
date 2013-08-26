package com.intel.cosbench.api.nio.client;

import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.util.Asserts;

import com.intel.cosbench.api.context.ExecContext;
import com.intel.cosbench.api.stats.StatsListener;
import com.intel.cosbench.api.validator.ResponseValidator;
import com.intel.cosbench.log.LogFactory;
import com.intel.cosbench.log.Logger;


/**
 * The future callback class especially for COSBench use.
 *    
 * @author ywang19
 *
 */
public class COSBFutureCallback implements FutureCallback<HttpResponse> {
	private RequestThrottler throttler;	// the throttler is to throttle request rate.
	private ExecContext context;  // data exchange for async response handling.	

	private ResponseValidator validator;
	private StatsListener collector;
    private static final Logger LOGGER = LogFactory.getSystemLogger();
	
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
	
	public COSBFutureCallback(final RequestThrottler throttler, ExecContext context, ResponseValidator validator, StatsListener collector) {		
		this(throttler, context, validator);
		this.collector = collector;		
	}
	
	public ResponseValidator getValidator() {
		return this.validator;
	}
	
//	public void setValidator(ResponseValidator validator) {
//		this.validator = validator;
//	}
	
//	public void setCollector(StatsListener collector) {
//		this.collector = collector;
//	}
	
//	public long countUp() {
//		return throttler.countUp();
//	}
	
	public long countDown() {
		return throttler.countDown();
	}
	
	public void await() throws InterruptedException {
		if(throttler != null)
			throttler.await();
	}

	@Override
    public void completed(final HttpResponse response) {    
		if(Thread.currentThread().getId() != context.threadId) {
			LOGGER.debug("COMPLETED: ThreadID mismatch with request thread = {} vs response thread = {}.", context.threadId, Thread.currentThread().getId());
		}
		
		context.setResponse(response);

		try {
			if(validator != null)
				context.setStatus(validator.validate(response, context));
    	}catch(Throwable e) {
    		LOGGER.error("Response can't pass validation with exception: " + e.getMessage());
    		e.printStackTrace();
    	}finally {
    		collector.onStats(context, context.getStatus());    	
            LOGGER.info("COMPLETED: " + context.getUri() + " --> " + response.getStatusLine() + ", length= " + context.getLength() + ", outstanding requests =  " + countDown()); 
    	}
    	
    }

	@Override
    public void failed(final Exception ex) {
		if(Thread.currentThread().getId() != context.threadId) {
			LOGGER.debug("COMPLETED: ThreadID mismatch with request thread = {} vs response thread = {}.", context.threadId, Thread.currentThread().getId());
		}
		
        ex.printStackTrace();
   		
        collector.onStats(context, false);  

        LOGGER.info("FAILED: {} --> {}, with " + countDown() + " outstanding requests.", context.getUri(), ex.getMessage()); 
    }

	@Override
    public void cancelled() {
		if(Thread.currentThread().getId() != context.threadId) {
			LOGGER.debug("COMPLETED: ThreadID mismatch with request thread = {} vs response thread = {}.", context.threadId, Thread.currentThread().getId());
		}

   		collector.onStats(context, false);  
        LOGGER.info("CANCELLED: {} --> {}, with " + countDown() + " outstanding requests.", context.getUri()); 
    }
    
}
