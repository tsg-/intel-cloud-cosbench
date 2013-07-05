package com.intel.cosbench.api.nioengine;

import java.util.concurrent.CountDownLatch;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;

public class COSBFutureCallback implements FutureCallback<HttpResponse> {
	private HttpHost target;
	
	private CountDownLatch latch;
	
	public COSBFutureCallback(CountDownLatch latch) {
		this.latch = latch;
	}
	
	public COSBFutureCallback(CountDownLatch latch, HttpHost target) {
		this.target = target;
		this.latch = latch;
	}
	
	public void setTarget(HttpHost target) {
		this.target = target;
	}
	
    public void completed(final HttpResponse response) {
    	latch.countDown();
        System.out.println("SUCCEED: " + target + "->" + response.getStatusLine());
    }

    public void failed(final Exception ex) {
    	latch.countDown();
        System.out.println("FAILED: " + target + "->" + ex);
    }

    public void cancelled() {
    	latch.countDown();
        System.out.println("CANCELLED: " + target + " cancelled");
    }
    
}
