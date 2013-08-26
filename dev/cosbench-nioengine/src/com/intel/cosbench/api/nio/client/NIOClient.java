package com.intel.cosbench.api.nio.client;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.nio.pool.BasicNIOConnPool;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.nio.protocol.BasicAsyncRequestProducer;
import org.apache.http.nio.protocol.HttpAsyncRequester;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.params.HttpParams;
import org.apache.http.params.SyncBasicHttpParams;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.protocol.HttpProcessor;
import org.apache.http.protocol.HttpProcessorBuilder;
import org.apache.http.protocol.RequestConnControl;
import org.apache.http.protocol.RequestContent;
import org.apache.http.protocol.RequestExpectContinue;
import org.apache.http.protocol.RequestTargetHost;
import org.apache.http.protocol.RequestUserAgent;
import org.apache.http.util.Asserts;

import com.intel.cosbench.api.context.ExecContext;
import com.intel.cosbench.api.ioengine.IOClient;
import com.intel.cosbench.api.nio.consumer.ConsumerBufferSink;
import com.intel.cosbench.api.nio.consumer.ConsumerFileSink;
import com.intel.cosbench.api.nio.consumer.ZCConsumer;
import com.intel.cosbench.api.nio.producer.*;
import com.intel.cosbench.api.stats.BaseStatsListener;
import com.intel.cosbench.api.stats.StatsListener;
import com.intel.cosbench.api.validator.BaseResponseValidator;
import com.intel.cosbench.api.validator.ResponseValidator;
import com.intel.cosbench.log.LogFactory;
import com.intel.cosbench.log.Logger;


/**
 * This class encapulates basic operations need for client to interact with NIO
 * engine.
 * 
 * @author ywang19
 * 
 */
@SuppressWarnings({ "deprecation", "unused" })
public class NIOClient implements IOClient {

	private BasicNIOConnPool connPool;
	private HttpAsyncRequester requester;
	
	private final RequestThrottler throttler;
	private ResponseValidator validator;
	private StatsListener collector;
	
	private final int BUFFER_SIZE;
    private static final Logger LOGGER = LogFactory.getSystemLogger();

    @Override
	public void setValidator(ResponseValidator validator) {
		this.validator = validator;
	}
	
    @Override
	public void setListener(StatsListener listener) {
		this.collector = listener;
	}

	public NIOClient(BasicNIOConnPool connPool, int concurrency)
	{
		Asserts.check(connPool != null, "Connection Pool shouldn't be null");
		Asserts.check(concurrency > 0, "concurrency must be a positive number");
		
		this.connPool = connPool;
		this.throttler = new RequestThrottler(concurrency);
//		this.validator = new BaseResponseValidator();
		this.collector = new BaseStatsListener();
		this.BUFFER_SIZE = 8192;
		
        HttpProcessor httpproc = HttpProcessorBuilder.create()
                // Use standard client-side protocol interceptors
                .add(new RequestContent())
                .add(new RequestTargetHost())
                .add(new RequestConnControl())
                .add(new RequestUserAgent("Mozilla/5.0"))
                .add(new RequestExpectContinue()).build();
        
        HttpParams params = new SyncBasicHttpParams();
        params
            .setIntParameter(CoreConnectionPNames.SO_TIMEOUT, 60000)
            .setIntParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 3000)
            .setIntParameter(CoreConnectionPNames.SOCKET_BUFFER_SIZE, BUFFER_SIZE)
            .setBooleanParameter(CoreConnectionPNames.STALE_CONNECTION_CHECK, false)
            .setBooleanParameter(CoreConnectionPNames.TCP_NODELAY, true)
            .setParameter(CoreProtocolPNames.ORIGIN_SERVER, "HttpTest/1.1")
        	.setParameter(CoreProtocolPNames.USER_AGENT, "COSBench/0.3");
//        	.setParameter(ConnRoutePNames.DEFAULT_PROXY, "http://proxy-prc.intel:com:911");
        
        this.requester = new HttpAsyncRequester(httpproc,
        								new DefaultConnectionReuseStrategy(),
        								params);
	}
	
	public NIOClient(BasicNIOConnPool connPool)
	{
		this(connPool, connPool.getMaxTotal());
	}
		
	public void await()
	{
		if(throttler != null) 
		{
			try{
				throttler.await();
			}catch(InterruptedException ie) {
				throttler.dispose();
			}
		}
	}
	
	public BasicHttpRequest makeHttpHead(String path)
	{
		BasicHttpRequest request = new BasicHttpRequest("HEAD", path);
		
		return request;
	}
	
	public BasicHttpRequest makeHttpGet(String path)
	{
		BasicHttpRequest request = new BasicHttpRequest("GET", path);
		
		return request;
	}
	
	public HttpEntityEnclosingRequest makeHttpPut(String path)
	{
		HttpEntityEnclosingRequest request = new BasicHttpEntityEnclosingRequest("PUT", path);
		
		return request;
	}
	
	public BasicHttpRequest makeHttpDelete(String path)
	{
		BasicHttpRequest request = new BasicHttpRequest("DELETE", path);
		
		return request;
	}
	
	public COSBFutureCallback makeFutureCallback(ExecContext context)
	{
		return new COSBFutureCallback(throttler, context, validator, collector);
	}

	public HttpResponse GETorHEAD(HttpHost target, HttpRequest request, ExecContext context) throws Exception {
		return GETorHEAD(target, request, context, false);
	}
	
	private HttpResponse inSync(Future<HttpResponse> future, COSBFutureCallback futureCallback) {
    	try {
        	HttpResponse response = future.get();
            futureCallback.completed(response); // getValidator().validate(response, context);      
            
            return response;
    	}catch(Exception ex) {
    		futureCallback.failed(ex);
    	}
    	
    	return null;
	}

	public HttpResponse GETorHEAD(HttpHost target, HttpRequest request, ExecContext context, final boolean blocking) throws Exception {
        // Create HTTP requester
    	HttpCoreContext coreContext = HttpCoreContext.create();
    	
    	final ZCConsumer<ByteBuffer> consumer = new ZCConsumer<ByteBuffer>(new ConsumerBufferSink(ByteBuffer.allocate(BUFFER_SIZE)));
   		
 		// initialize future callback.
//    	COSBFutureCallback futureCallback = makeFutureCallback(new ExecContext(target, request, null, context.operator, 0));
 		COSBFutureCallback futureCallback = makeFutureCallback(context);
        
    	Future<HttpResponse> future = requester.execute(
                new BasicAsyncRequestProducer(target, request),
                consumer,
                connPool,
                coreContext,
                futureCallback);
        
        if(future.isDone()) {
	    	LOGGER.info("Request is done.");
	    	return future.get();
	    }
        
        if(blocking)
        {
        	try {
	        	HttpResponse response = future.get();
	            futureCallback.completed(response); // getValidator().validate(response, context);      
	            
	            return response;
        	}catch(ExecutionException ex) {
        		futureCallback.failed(ex);
        	}catch(CancellationException ex) {
        		futureCallback.cancelled();
        	}
        }
         
        return null;
    }

	public HttpResponse PUT(HttpHost target, HttpEntityEnclosingRequest request, ExecContext context) throws Exception {
		return PUT(target, request, context, false);
	}
	
	public HttpResponse PUT(HttpHost target, HttpEntityEnclosingRequest request, ExecContext context, final boolean blocking) throws Exception {
	
    	HttpCoreContext coreContext = HttpCoreContext.create();
    	String uri = request.getRequestLine().getUri();

//    	String up_path = doc_root + "/upload/" + uri;
//		String down_path = doc_root + "/download/" + uri;
//    	final ZCConsumer<File> consumer = new ZCConsumer<File>(new ConsumerFileSink(new File(down_path)));        	
    	
    	final ZCConsumer<ByteBuffer> consumer = new ZCConsumer<ByteBuffer>(new ConsumerBufferSink(ByteBuffer.allocate(BUFFER_SIZE)));
         // for buffer based producer:
 		final Random random = new Random(System.currentTimeMillis());
        final ContentType contentType = ContentType.TEXT_PLAIN;
        LOGGER.info("Uploading Object with size={}", context.getLength());
 		ZCProducer<ByteBuffer> producer = new ZCProducer<ByteBuffer>(context.getLength() > 0? new ProducerBufferSource(random, context.getLength()) : null);
 		request.setEntity(producer.getEntity());
    	
 		// initialize future callback.
 		COSBFutureCallback futureCallback = makeFutureCallback(context);
        Future<HttpResponse> future = requester.execute(
        		new BaseZCAsyncRequestProducer(target, request, producer),
                consumer,
                connPool,
                coreContext,
                futureCallback);
        
	    if(future.isDone()) {
	    	LOGGER.info("Request is done.");
	    	return future.get();
	    }
	    
	    if(blocking)
	    {
        	try {
	        	HttpResponse response = future.get();
	            futureCallback.completed(response); // getValidator().validate(response, context);      
	            
	            return response;
        	}catch(ExecutionException ex) {
        		futureCallback.failed(ex);
        	}catch(CancellationException ex) {
        		futureCallback.cancelled();
        	}
	    }
			
	    return null;
    }

	public HttpResponse DELETE(HttpHost target, HttpRequest request, ExecContext context) throws Exception {
		return DELETE(target, request, context, false);
	}

	public HttpResponse DELETE(HttpHost target, HttpRequest request, ExecContext context, final boolean blocking) throws Exception {
    	
    	HttpCoreContext coreContext = HttpCoreContext.create();
    	String uri = request.getRequestLine().getUri();
    	
//    	String down_path = doc_root + "/" + uri;    	
//    	final ZCConsumer<File> consumer = new ZCConsumer<File>(new ConsumerFileSink(new File(down_path)));        	
    	final ZCConsumer<ByteBuffer> consumer = new ZCConsumer<ByteBuffer>(new ConsumerBufferSink(ByteBuffer.allocate(BUFFER_SIZE)));

 		// initialize future callback.
    	COSBFutureCallback futureCallback = makeFutureCallback(context);

        Future<HttpResponse> future = requester.execute(
        		new BasicAsyncRequestProducer(target, request),
                consumer,
                connPool,
                coreContext,
                futureCallback);

        if(future.isDone()) {
        	LOGGER.info("Request is done.");
        	return future.get();
        }
        
        if(blocking)
        {
        	try {
	        	HttpResponse response = future.get();
	            futureCallback.completed(response); // getValidator().validate(response, context);      
	            
	            return response;
        	}catch(ExecutionException ex) {
        		futureCallback.failed(ex);
        	}catch(CancellationException ex) {
        		futureCallback.cancelled();
        	}
        }
        
		return null;
    }

//	@Override
//	public void init(ResponseValidator validator, StatsListener collector) {
//		this.validator = validator;
//		this.collector = collector;
//	}

}
