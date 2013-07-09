package com.intel.cosbench.api.nio.client;

import java.io.File;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.Future;

import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.nio.pool.BasicNIOConnPool;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.nio.protocol.BasicAsyncRequestProducer;
import org.apache.http.nio.protocol.HttpAsyncRequester;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.protocol.HttpProcessor;
import org.apache.http.protocol.HttpProcessorBuilder;
import org.apache.http.protocol.RequestConnControl;
import org.apache.http.protocol.RequestContent;
import org.apache.http.protocol.RequestExpectContinue;
import org.apache.http.protocol.RequestTargetHost;
import org.apache.http.protocol.RequestUserAgent;
import org.apache.http.util.Asserts;

import com.intel.cosbench.api.nio.consumer.ConsumerFileSink;
import com.intel.cosbench.api.nio.consumer.ZCConsumer;
import com.intel.cosbench.api.nio.producer.BaseZCAsyncRequestProducer;
import com.intel.cosbench.api.nio.producer.BaseZCProducer;
import com.intel.cosbench.api.nio.producer.ProducerBufferSource;
import com.intel.cosbench.api.nio.producer.ZCProducer;


/**
 * This class encapulates basic operations need for client to interact with NIO
 * engine.
 * 
 * @author ywang19
 * 
 */
public class NIOClient {

	private BasicNIOConnPool connPool;
	private HttpAsyncRequester requester;
//	private CountDownLatch latch;
	private COSBFutureCallback futureCallback;

	private String doc_root = "c:/temp/download/";
	
	public NIOClient(BasicNIOConnPool connPool, int concurrency)
	{
		Asserts.check(concurrency > 0, "concurrency must be a positive number");
		this.connPool = connPool;
    	futureCallback =  new COSBFutureCallback(concurrency);
		
        HttpProcessor httpproc = HttpProcessorBuilder.create()
                // Use standard client-side protocol interceptors
                .add(new RequestContent())
                .add(new RequestTargetHost())
                .add(new RequestConnControl())
                .add(new RequestUserAgent("Mozilla/5.0"))
                .add(new RequestExpectContinue()).build();
        
        this.requester = new HttpAsyncRequester(httpproc);
	}
	
	public NIOClient(BasicNIOConnPool connPool)
	{
		this.connPool = connPool;
		System.out.println("Max Conn Pool = " + connPool.getMaxTotal() + "\t Max Per Route = " + connPool.getDefaultMaxPerRoute());
    	futureCallback =  new COSBFutureCallback(connPool.getMaxTotal());
		
        HttpProcessor httpproc = HttpProcessorBuilder.create()
                // Use standard client-side protocol interceptors
                .add(new RequestContent())
                .add(new RequestTargetHost())
                .add(new RequestConnControl())
                .add(new RequestUserAgent("Mozilla/5.0"))
                .add(new RequestExpectContinue()).build();
        
        this.requester = new HttpAsyncRequester(httpproc);
	}
	
	public void await() throws InterruptedException
	{
		if(futureCallback != null)
			futureCallback.await();
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
	
	public void GET(HttpHost target, HttpRequest request) throws Exception {
        // Create HTTP requester
    	long start = System.currentTimeMillis();
    	
    	HttpCoreContext coreContext = HttpCoreContext.create();
    	String uri = request.getRequestLine().getUri();
    	String down_path = doc_root + "/" + uri;
    	
    	final ZCConsumer<File> consumer = new ZCConsumer<File>(new ConsumerFileSink(new File(down_path)));        	
//    	final ZCConsumer<ByteBuffer> consumer = new ZCConsumer<ByteBuffer>(new ConsumerNullSink(ByteBuffer.allocate(8192)));
   		
 		// initialize future callback.
		futureCallback.setTarget(target);
    	futureCallback.countUp();
        Future<HttpResponse> future = requester.execute(
                new BasicAsyncRequestProducer(target, request),
                consumer,
                connPool,
                coreContext,
                futureCallback);

        if(future.isDone()) {
        	System.out.println("Request is done.");
        }
        	
//        future.get();
        
        long end = System.currentTimeMillis();
        
        System.out.println("Elapsed Time: " + (end-start) + " ms.");
    }

	public void GET_withWait(HttpHost target, HttpRequest request) throws Exception {
        // Create HTTP requester
    	long start = System.currentTimeMillis();
    	
    	HttpCoreContext coreContext = HttpCoreContext.create();
    	String uri = request.getRequestLine().getUri();
    	String down_path = doc_root + "/" + uri;
    	
    	final ZCConsumer<File> consumer = new ZCConsumer<File>(new ConsumerFileSink(new File(down_path)));        	
//    	final ZCConsumer<ByteBuffer> consumer = new ZCConsumer<ByteBuffer>(new ConsumerNullSink(ByteBuffer.allocate(8192)));
   		
 		// initialize future callback.
		futureCallback.setTarget(target);
    	futureCallback.countUp();
        Future<HttpResponse> future = requester.execute(
                new BasicAsyncRequestProducer(target, request),
                consumer,
                connPool,
                coreContext,
                futureCallback);

        if(future.isDone()) {
        	System.out.println("Request is done.");
        }
        	
        future.get();
        
        long end = System.currentTimeMillis();
        
        System.out.println("Elapsed Time: " + (end-start) + " ms.");
    }

	public void PUT(HttpHost target, HttpEntityEnclosingRequest request) throws Exception {
	
    	long start = System.currentTimeMillis();
    	
    	HttpCoreContext coreContext = HttpCoreContext.create();
    	String uri = request.getRequestLine().getUri();
    	String down_path = "c:/temp/download/" + uri;
    	String up_path = "c:/temp/upload/" + uri;
    	final ZCConsumer<File> consumer = new ZCConsumer<File>(new ConsumerFileSink(new File(down_path)));        	
//    	final ZCConsumer<ByteBuffer> consumer = new ZCConsumer<ByteBuffer>(new ConsumerBufferSink(ByteBuffer.allocate(8192)));
    	

         final ContentType contentType = ContentType.TEXT_PLAIN;
         
         // for File based producer.
// 		final File file = new File(up_path);
//		ZCProducer<File> producer = null;
// 		if (file.canRead()) {
// 			producer = new ZCProducer<File>(new ProducerFileSource(file),
// 					URI.create(up_path), contentType);
// 		}

         // for buffer based producer:
 		Random random = new Random(26);
 		ZCProducer<ByteBuffer> producer = new ZCProducer<ByteBuffer>(new ProducerBufferSource(random, 1024*128), URI.create(up_path), contentType);
 		
 		request.setEntity(producer.getEntity());
    	
 		// initialize future callback.
		futureCallback.setTarget(target);
    	futureCallback.countUp();
        Future<HttpResponse> future = requester.execute(
        		new BaseZCAsyncRequestProducer(target, request, producer),
                consumer,
//                new BasicAsyncResponseConsumer() ,
                connPool,
                coreContext,
                // Handle HTTP response from a callback
                futureCallback);
        
        if(future.isDone()) {
        	System.out.println("Request is done.");
        }
        	
//        future.get();
        
        long end = System.currentTimeMillis();
        
        System.out.println("Elapsed Time: " + (end-start) + " ms.");
    }
	
	public void DELETE(HttpHost target, HttpRequest request) throws Exception {
        // Create HTTP requester
    	long start = System.currentTimeMillis();
    	
    	HttpCoreContext coreContext = HttpCoreContext.create();
    	String uri = request.getRequestLine().getUri();
    	String down_path = doc_root + "/" + uri;
    	
    	final ZCConsumer<File> consumer = new ZCConsumer<File>(new ConsumerFileSink(new File(down_path)));        	
   		
 		// initialize future callback.
		futureCallback.setTarget(target);
    	futureCallback.countUp();
        Future<HttpResponse> future = requester.execute(
        		new BasicAsyncRequestProducer(target, request),
                consumer,
                connPool,
                coreContext,
                futureCallback);

        if(future.isDone()) {
        	System.out.println("Request is done.");
        }
        	
//        future.get();
        
        long end = System.currentTimeMillis();
        
        System.out.println("Elapsed Time: " + (end-start) + " ms.");
    }


}
