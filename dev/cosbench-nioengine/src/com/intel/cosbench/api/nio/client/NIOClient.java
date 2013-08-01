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
import com.intel.cosbench.api.nio.producer.BaseZCAsyncRequestProducer;
import com.intel.cosbench.api.nio.producer.ProducerBufferSource;
import com.intel.cosbench.api.nio.producer.ZCProducer;
import com.intel.cosbench.api.stats.BaseStatsCollector;
import com.intel.cosbench.api.stats.StatsCollector;
import com.intel.cosbench.api.validator.BaseResponseValidator;
import com.intel.cosbench.api.validator.ResponseValidator;


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
	private StatsCollector collector;
	
	private final String doc_root = "./doc_root";

	public ResponseValidator getValidator() {
		return validator;
	}

	public void setValidator(ResponseValidator validator) {
		this.validator = validator;
	}
	
	public StatsCollector getCollector() {
		return collector;
	}

	public void setCollector(StatsCollector collector) {
		this.collector = collector;
	}

	public NIOClient(BasicNIOConnPool connPool, int concurrency)
	{
		Asserts.check(connPool != null, "Connection Pool shouldn't be null");
		Asserts.check(concurrency > 0, "concurrency must be a positive number");
		
		this.connPool = connPool;
		this.throttler = new RequestThrottler(concurrency);
		this.validator = new BaseResponseValidator();
		this.collector = new BaseStatsCollector();
		
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
            .setIntParameter(CoreConnectionPNames.SOCKET_BUFFER_SIZE, 8 * 1024)
            .setBooleanParameter(CoreConnectionPNames.STALE_CONNECTION_CHECK, false)
            .setBooleanParameter(CoreConnectionPNames.TCP_NODELAY, true)
            .setParameter(CoreProtocolPNames.ORIGIN_SERVER, "HttpTest/1.1")
        	.setParameter(CoreProtocolPNames.USER_AGENT, "AsynCore/1.1");
//        	.setParameter(ConnRoutePNames.DEFAULT_PROXY, "http://proxy-prc.intel:com:911");
        
        this.requester = new HttpAsyncRequester(httpproc,
        								new DefaultConnectionReuseStrategy(),
        								params);
	}
	
	public NIOClient(BasicNIOConnPool connPool)
	{
		this(connPool, connPool.getMaxTotal());
	}
		
	public void await() throws InterruptedException
	{
		if(throttler != null)
			throttler.await();
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
	
	public void GET(HttpHost target, HttpRequest request) throws Exception {
        // Create HTTP requester
//    	long start = System.currentTimeMillis();
    	HttpCoreContext coreContext = HttpCoreContext.create();
    	String uri = request.getRequestLine().getUri();
    	String down_path = doc_root + "/download/" + uri;
    	
//    	final ZCConsumer<File> consumer = new ZCConsumer<File>(new ConsumerFileSink(new File(down_path)));        	
    	final ZCConsumer<ByteBuffer> consumer = new ZCConsumer<ByteBuffer>(new ConsumerBufferSink(ByteBuffer.allocate(8192)));
   		
 		// initialize future callback.
    	COSBFutureCallback futureCallback = makeFutureCallback(new ExecContext(target, request, null));
//    	futureCallback.countUp();
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
//        long end = System.currentTimeMillis();
//        System.out.println("Elapsed Time: " + (end-start) + " ms.");
    }

	public void GET_withWait(HttpHost target, HttpRequest request) throws Exception {
        // Create HTTP requester
//    	long start = System.currentTimeMillis();
    	
    	HttpCoreContext coreContext = HttpCoreContext.create();
    	String uri = request.getRequestLine().getUri();
    	String down_path = doc_root + "/download/" + uri;
    	
//    	final ZCConsumer<File> consumer = new ZCConsumer<File>(new ConsumerFileSink(new File(down_path)));        	
    	final ZCConsumer<ByteBuffer> consumer = new ZCConsumer<ByteBuffer>(new ConsumerBufferSink(ByteBuffer.allocate(8192)));
   		
 		// initialize future callback.
    	COSBFutureCallback futureCallback = makeFutureCallback(new ExecContext(target, request, null));
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
        
//        long end = System.currentTimeMillis();
//        
//        System.out.println("Elapsed Time: " + (end-start) + " ms.");
    }

	public void PUT(HttpHost target, HttpEntityEnclosingRequest request) throws Exception {
	
//    	long start = System.currentTimeMillis();
    	
    	HttpCoreContext coreContext = HttpCoreContext.create();
    	String uri = request.getRequestLine().getUri();
    	String down_path = doc_root + "/download/" + uri;
    	String up_path = doc_root + "/upload/" + uri;
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
 		COSBFutureCallback futureCallback = makeFutureCallback(new ExecContext(target, request, null));
//		futureCallback.setTarget(target);
//    	futureCallback.countUp();
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
        
//        long end = System.currentTimeMillis();
//        
//        System.out.println("Elapsed Time: " + (end-start) + " ms.");
    }
	
	public void DELETE(HttpHost target, HttpRequest request) throws Exception {
        // Create HTTP requester
//    	long start = System.currentTimeMillis();
    	
    	HttpCoreContext coreContext = HttpCoreContext.create();
    	String uri = request.getRequestLine().getUri();
    	String down_path = doc_root + "/" + uri;
    	
    	final ZCConsumer<File> consumer = new ZCConsumer<File>(new ConsumerFileSink(new File(down_path)));        	
   		
 		// initialize future callback.
    	COSBFutureCallback futureCallback = makeFutureCallback(new ExecContext(target, request, null));
//		futureCallback.setTarget(target);
//    	futureCallback.countUp();
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
        
//        long end = System.currentTimeMillis();
//        
//        System.out.println("Elapsed Time: " + (end-start) + " ms.");
    }

	@Override
	public void init(ResponseValidator validator, StatsCollector collector) {
		this.validator = validator;
		this.collector = collector;
	}

}
