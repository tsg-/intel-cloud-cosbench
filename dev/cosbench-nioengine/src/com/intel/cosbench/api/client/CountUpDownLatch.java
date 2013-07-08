package com.intel.cosbench.api.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class CountUpDownLatch {
    CountDownLatch latch;
    AtomicLong    count;

    public CountUpDownLatch(long count) {
        this.latch = new CountDownLatch(1);
        this.count = new AtomicLong(count);
    }

    public void await() throws InterruptedException {
        if (count.compareAndSet(0, 0)) {
            return;
        }

        latch.await();
    }
    
    private boolean isEmpty() {
    	return count.compareAndSet(0, 0);
    }

    public long countDown() {
    	count.decrementAndGet();
        if(isEmpty()) {
            latch.countDown();
        }
        
        return count.get();
    }

    public long getCount() {
        return count.get();
    }

    public long countUp() {

        if (latch.getCount() == 0) {
            latch = new CountDownLatch(1);
        }

        return count.incrementAndGet();
    }

    public void setCount(int count) {

        if (isEmpty()) {
            if (latch.getCount() != 0) {
                latch.countDown();
            }
        } else if (latch.getCount() == 0) {
            latch = new CountDownLatch(1);
        }

        this.count.set(count);
    }
}