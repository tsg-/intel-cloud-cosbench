package com.intel.cosbench.api.nioengine;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;

public class COSBFutureCallback implements FutureCallback<HttpResponse> {
	private HttpHost target;
	private CountUpDownLatch latch;
	
	public COSBFutureCallback(long count) {
		this.latch = new CountUpDownLatch(count);
	}
	
	public COSBFutureCallback(long count, HttpHost target) {
		this(count);
		this.target = target;
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
	
	public void setTarget(HttpHost target) {
		this.target = target;
	}
	
    public void completed(final HttpResponse response) {
//    	long ql = latch.countDown();
        System.out.println("SUCCEED: " + target + "->" + response.getStatusLine() + "\t Outstanding Request is " + latch.countDown());
    }

    public void failed(final Exception ex) {
//    	latch.countDown();
        System.out.println("FAILED: " + target + "->" + ex + "\t Outstanding Request is " + latch.countDown());
    }

    public void cancelled() {
//    	latch.countDown();
        System.out.println("CANCELLED: " + target + " cancelled" + "\t Outstanding Request is " + latch.countDown());
    }
    
}
