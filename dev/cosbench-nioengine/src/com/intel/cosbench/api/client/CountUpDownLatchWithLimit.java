package com.intel.cosbench.api.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

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
    
    private boolean isEmpty() {
    	return sem.availablePermits() == limit;
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
        	System.out.println("Acquire Timestamp = " + (System.currentTimeMillis() - enter));
        }catch(InterruptedException ie) {// expect to be interrupted by main thread when termination.
        	sem.release(limit);
        	latch.countDown();
        }
        return getActiveCount();
    }

}
