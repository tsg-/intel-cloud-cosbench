package com.intel.cosbench.api.nioengine;

import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.nio.entity.HttpAsyncContentProducer;
import org.apache.http.nio.protocol.BasicAsyncRequestProducer;

public class BaseZCAsyncRequestProducer extends BasicAsyncRequestProducer {
    public BaseZCAsyncRequestProducer(HttpHost target, HttpEntityEnclosingRequest request, HttpAsyncContentProducer producer)
    {
    	super(target, request, producer);
    }
}
