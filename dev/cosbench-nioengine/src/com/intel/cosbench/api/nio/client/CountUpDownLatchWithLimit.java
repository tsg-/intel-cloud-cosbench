package com.intel.cosbench.api.nio.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

import com.intel.cosbench.log.LogFactory;
import com.intel.cosbench.log.Logger;

/**
 * This class helps to track the request issuing and completion, it also helps to throttle request rate not to over preset concurrency limit.
 * 
 * @author ywang19
 *
 */
public class CountUpDownLatchWithLimit {
    CountDownLatch latch;
    final int limit;
    private final Semaphore sem;
    
    private static final Logger LOGGER = LogFactory.getSystemLogger();

    public CountUpDownLatchWithLimit(int limit) {
        this.latch = new CountDownLatch(1);
        this.sem = new Semaphore((int) limit, true);
        this.limit = limit;
    }

    public void await() throws InterruptedException {
        if (isEmpty()) {
            return;
        }

        latch.await();
    }
    
    public void dispose() {
    	sem.release();
    	latch.countDown();    	
    }
    
    private boolean isEmpty() {
    	return sem.availablePermits() >= limit;
    }
    
    public long countDown() {
    	sem.release();

    	if(isEmpty()) {
            latch.countDown();
        }
    	
        return getActiveCount();
    }

    public long getActiveCount() {
        return limit - sem.availablePermits();
    }

    public long countUp() {
        if (latch.getCount() == 0) {
            latch = new CountDownLatch(1);
        }

        try{
        	long enter = System.currentTimeMillis();
        	sem.acquire();
        	LOGGER.debug("Acquiring latch takes {} milliseconds.", (System.currentTimeMillis() - enter));
        }catch(InterruptedException ie) {// expect to be interrupted by main thread when termination.
        	sem.release(limit);
        	latch.countDown();
        }

        return getActiveCount();
    }

}
