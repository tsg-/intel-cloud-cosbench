package com.intel.cosbench.api.nio.client;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;


/**
 * The future callback class especially for COSBench use.
 * 
 * @author ywang19
 *
 */
public class COSBFutureCallback implements FutureCallback<HttpResponse> {
	private HttpHost target;
	private CountUpDownLatchWithLimit latch;
	
	public COSBFutureCallback(int count) {
		this.latch = new CountUpDownLatchWithLimit(count);
	}
	
	public COSBFutureCallback(int count, HttpHost target) {
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
