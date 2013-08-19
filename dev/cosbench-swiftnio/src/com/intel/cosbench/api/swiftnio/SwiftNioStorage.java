/** 
 
Copyright 2013 Intel Corporation, All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. 
*/ 

package com.intel.cosbench.api.swiftnio;

import static com.intel.cosbench.client.swiftnio.SwiftConstants.*;

import java.io.*;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.Map;
import java.util.Random;

import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;

import com.intel.cosbench.api.nio.client.NIOClient;

import com.intel.cosbench.api.context.ExecContext;
import com.intel.cosbench.api.nio.engine.NIOEngine;
import com.intel.cosbench.api.stats.StatsListener;
import com.intel.cosbench.api.storage.*;
//import com.intel.cosbench.client.http.HttpClientUtil;
import com.intel.cosbench.client.swiftnio.*;
import com.intel.cosbench.config.Config;
import com.intel.cosbench.log.LogFactory;
import com.intel.cosbench.log.LogManager;
import com.intel.cosbench.log.Logger;

/**
 * This class encapsulates a Swift implementation for Storage API.
 * 
 * @author ywang19
 * 
 */
class SwiftNioStorage extends NoneStorage {
	/* configurations */
//    private int timeout; // connection and socket timeout
    
    /* current operation */
    private volatile HttpRequest method;
    
    /* user context */
//    private volatile String authToken;
//    private volatile String storageURL;

    private NIOClient nioclient;
    

    @Override
    public void init(Config config, Logger logger) {
        super.init(config, logger);

        parms.put(CONN_TIMEOUT_KEY, config.getInt(CONN_TIMEOUT_KEY, CONN_TIMEOUT_DEFAULT));
        parms.put(AUTH_TOKEN_KEY, config.get(AUTH_TOKEN_KEY, AUTH_TOKEN_DEFAULT));
        parms.put(STORAGE_URL_KEY,  config.get(STORAGE_URL_KEY, STORAGE_URL_DEFAULT));
        
        logger.debug("using storage config: {}", parms);

        if(ioengine != null) {
        	nioclient = (NIOClient)ioengine.newClient();
        	nioclient.setValidator(new SwiftResponseValidator());
        	logger.info("swift client has been initialized");
        }else {
            logger.error("swift i/o engine is not correctly initialized, please check it first.");
        }

    }
    
//    @Override
    public void setListener(StatsListener collector) {
    	this.listener = collector;
		if(nioclient != null)
			nioclient.setListener(collector);
    }
//
//    @Override
//    public void initValidator(ResponseValidator validator) {
//    	this.validator = validator;
//		if(nioclient != null)
//			nioclient.setValidator(validator);
//    }
    
//	public void setValidator(ResponseValidator validator) {
//		if(nioclient != null)
//			nioclient.setValidator(validator);
//	}

//	public void setCollector(StatsListener collector) {
//		if(nioclient != null)
//			nioclient.setCollector(collector);
//	}

	@Override
    public void setAuthContext(ExecContext info) {
        super.setAuthContext(info);
        logger.debug("using storage config: {}", parms);
        
//		int statusCode = response.getStatusLine().getStatusCode();
//		
//		 if ((response.getStatusLine().getStatusCode() >= HttpStatus.SC_OK) &&
//	 				(response.getStatusLine().getStatusCode() < (HttpStatus.SC_OK + 100))
//	 				) 
//		 {
//			String authToken = response.getFirstHeader(X_AUTH_TOKEN) != null ? response
//			         .getFirstHeader(X_AUTH_TOKEN).getValue() : "AUTH_xxx";
//			String storageURL = response.getFirstHeader(X_STORAGE_URL) != null ? response
//			         .getFirstHeader(X_STORAGE_URL).getValue() : "http://127.0.0.1:8080/";
//			context.put(AUTH_TOKEN_KEY, authToken);
//			context.put(STORAGE_URL_KEY, storageURL);
//
//			EntityUtils.consume(response.getEntity()); 
//			
//			return true;
//        }

    }

    @Override
    public void dispose() {
        super.dispose();
        nioclient.await();
    }

    @Override
    public void abort() {
        super.abort();
//        client.abort();
    }

    public static void testGetObject()
    {
       	NIOEngine ioengine = new NIOEngine();

    	LogManager manager = LogFactory.createLogManager();
        Logger logger = manager.getLogger();
        	
    	ioengine.init(null,logger);
    	ioengine.startup();
    	
    	
    	NIOClient ioclient = ioengine.newClient();
    	
    	SwiftNioStorage storage = new SwiftNioStorage();
    	storage.initIOEngine(ioengine);
    	storage.init(null, logger);
    	
    	try
    	{
            String container = "128KB";        	
            Random rnd = new Random(23);            
            int range = 10;
            
            for(int i=0; i< 100; i++)
            {
	            int idx = (rnd.nextInt(range)+1);
	        	
	            String object = idx + ".txt";
	        	String path = "/" + container + "/" + idx + ".txt";
	        	System.out.println("Path=" + path);
	            System.out.println("[" + (i+1) + "]" + " Start Timestamp=" + System.currentTimeMillis());
	            
//	            storage.setAuthContext(new ExecContext());
//	            storage.getObject("read", container, object, null);
	            
	            System.out.println("[" + (i+1) + "]" + " End Timestamp=" + System.currentTimeMillis());
            }
            
            ioclient.await();
    	}catch(Exception ie) {
    		ie.printStackTrace();
    	}
    	
    	ioengine.shutdown();   	
    }
    
    public static void testPutObject()
    {
       	NIOEngine ioengine = new NIOEngine();

    	LogManager manager = LogFactory.createLogManager();
        Logger logger = manager.getLogger();
        	
    	ioengine.init(null,logger);
    	ioengine.startup();
    	
    	
    	NIOClient ioclient = ioengine.newClient();
    	
    	SwiftNioStorage storage = new SwiftNioStorage();
    	storage.initIOEngine(ioengine);
    	storage.init(null, logger);
    	
    	try
    	{
            String container = "128KB";        	
            Random rnd = new Random(23);            
            int range = 10;
            int size = 1024 *1024;
            
            for(int i=0; i< 100; i++)
            {
	            int idx = (rnd.nextInt(range)+1);
	        	
	            String object = idx + ".txt";
	        	String path = "/" + container + "/" + idx + ".txt";
	        	System.out.println("Path=" + path);
	            System.out.println("[" + (i+1) + "]" + " Start Timestamp=" + System.currentTimeMillis());
	            
//	            storage.setAuthContext(new Context());
//	            storage.createObject("write", container, object, null, size, null);
	            
	            System.out.println("[" + (i+1) + "]" + " End Timestamp=" + System.currentTimeMillis());
            }
            
            ioclient.await();
    	}catch(Exception ie) {
    		ie.printStackTrace();
    	}
    	
    	ioengine.shutdown();   	
    }
    
    public static void main(String[] args)
    {
    	testPutObject();
    	
    	testGetObject();
    	
    }
    
    @Override
    public void getObject(final String opType, String container, String object, Config config) throws StorageException, IOException {
        super.getObject(opType, container, object, config);

        HttpResponse response = null;
        
		if(nioclient == null)
		{
			logger.error("nio client is not initialized yet!");
			return;
		}

        try {              	
        	// construct request.
        	logger.debug("StorageURL = " + parms.getStr(STORAGE_URL_KEY, STORAGE_URL_DEFAULT));
        	URI uri = URI.create(parms.getStr(STORAGE_URL_KEY, STORAGE_URL_DEFAULT));        	
        	method = nioclient.makeHttpGet(uri.getPath() + "/" + container + "/" + object);
        	method.setHeader(X_AUTH_TOKEN, parms.getStr(AUTH_TOKEN_KEY, AUTH_TOKEN_DEFAULT));              	
            // issue request.
    		HttpHost target = new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme());
    		
    		ExecContext context = new ExecContext(target, method, null, opType, 0);
            response = nioclient.GETorHEAD(target, method, context, false);
    
        } catch (SocketTimeoutException ste) {
            throw new StorageTimeoutException(ste);
        } catch (InterruptedIOException ie) {
            throw new StorageInterruptedException(ie);
        } catch (SwiftException se) {
            String msg = se.getHttpStatusLine().toString();
            throw new StorageException(msg, se);
        } catch (Exception e) {
        	e.printStackTrace();
            throw new StorageException(e);
        }finally {
            if (response != null)
                EntityUtils.consume(response.getEntity());
        }
    }

    private boolean containerExists(final String opType, String container) throws IOException
    {
    	HttpResponse response = null;
    	
    	try {
            // construct request.
        	URI uri = URI.create(parms.getStr(STORAGE_URL_KEY, STORAGE_URL_DEFAULT));        	
        	method = nioclient.makeHttpHead(uri.getPath() + "/" + container);
        	method.setHeader(X_AUTH_TOKEN, parms.getStr(AUTH_TOKEN_KEY, AUTH_TOKEN_DEFAULT));              	
            // issue request.
    		HttpHost target = new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme());
    		
    		ExecContext context = new ExecContext(target, method, null, opType, 0);
//    		((SwiftResponseValidator)this.validator).exclusiveCode = HttpStatus.SC_NOT_FOUND;
    		response = nioclient.GETorHEAD(target, method, context, true);
//    		validator.validate(response, context);
    		
    		return context.status;
    		
		} catch(Exception e) {
			return false;
		} finally {
            if (response != null)
                EntityUtils.consume(response.getEntity());
        }
    }
    
    @Override
    public void createContainer(final String opType, String container, Config config) throws StorageException, IOException {
        super.createContainer(opType, container, config);

    	HttpResponse response = null;
    	
        try {
	        // construct request.
//	        if (containerExists(opType, container))
//	        	return;

	        // construct request.
	    	URI uri = URI.create(parms.getStr(STORAGE_URL_KEY, STORAGE_URL_DEFAULT));        	
	    	method = (HttpEntityEnclosingRequest)nioclient.makeHttpPut(uri.getPath() + "/" + container);
	    	method.setHeader(X_AUTH_TOKEN, parms.getStr(AUTH_TOKEN_KEY, AUTH_TOKEN_DEFAULT));              	
	        // issue request.
			HttpHost target = new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme());
			
    		ExecContext context = new ExecContext(target, method, null, opType, 0);
			response = nioclient.PUT(target, (HttpEntityEnclosingRequest)method, context);
			
        } catch (SocketTimeoutException ste) {
            throw new StorageTimeoutException(ste);
        } catch (InterruptedIOException ie) {
            throw new StorageInterruptedException(ie);
        } catch (SwiftException se) {
            String msg = se.getHttpStatusLine().toString();
            throw new StorageException(msg, se);
        } catch (Exception e) {
        	e.printStackTrace();
            throw new StorageException(e);
        } finally {
            if (response != null)
                EntityUtils.consume(response.getEntity());
        }
    }

    @Deprecated
    public void createObject(final String opType, String container, String object, byte[] data,
            Config config) throws StorageException, IOException {
        super.createObject(opType, container, object, data, config);
//        try {
//            client.storeObject(container, object, data);
//        } catch (SocketTimeoutException ste) {
//            throw new StorageTimeoutException(ste);
//        } catch (InterruptedIOException ie) {
//            throw new StorageInterruptedException(ie);
//        } catch (SwiftException se) {
//            String msg = se.getHttpStatusLine().toString();
//            throw new StorageException(msg, se);
//        } catch (Exception e) {
//            throw new StorageException(e);
//        }
    }

    @Override
    public void createObject(final String opType, String container, String object, InputStream data,
            long length, Config config) throws StorageException, IOException {
        super.createObject(opType, container, object, data, length, config);
        
        HttpResponse response = null;
        
		if(nioclient == null)
		{
			logger.error("nio client is not initialized yet!");
			return;
		}
        
        try {
        	// construct request.
        	URI uri = URI.create(parms.getStr(STORAGE_URL_KEY, STORAGE_URL_DEFAULT));   
        	HttpEntityEnclosingRequest method = nioclient.makeHttpPut(uri.getPath() + "/" + container + "/" + object);
        	this.method = method;
            method.setHeader(X_AUTH_TOKEN, parms.getStr(AUTH_TOKEN_KEY, AUTH_TOKEN_DEFAULT));
            
            // issue request.
    		HttpHost target = new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme());
    		
    		ExecContext context = new ExecContext(target, method, null, opType, length);
            response = nioclient.PUT(target, method, context);
            
        } catch (SocketTimeoutException ste) {
            throw new StorageTimeoutException(ste);
        } catch (InterruptedIOException ie) {
            throw new StorageInterruptedException(ie);
        } catch (SwiftException se) {
            String msg = se.getHttpStatusLine().toString();
            throw new StorageException(msg, se);
        } catch (Exception e) {
        	e.printStackTrace();
            throw new StorageException(e);
        } finally {
            if (response != null)
                EntityUtils.consume(response.getEntity());
        }
    }

    @Override
    public void deleteContainer(final String opType, String container, Config config) throws StorageException, IOException {
        super.deleteContainer(opType, container, config);
        
        HttpResponse response = null;
		if(nioclient == null)
		{
			logger.error("nio client is not initialized yet!");
			return;
		}
        
        try {
        	// construct request.
        	URI uri = URI.create(parms.getStr(STORAGE_URL_KEY, STORAGE_URL_DEFAULT));   
        	method = nioclient.makeHttpDelete(uri.getPath() + "/" + container);
            method.setHeader(X_AUTH_TOKEN, parms.getStr(AUTH_TOKEN_KEY, AUTH_TOKEN_DEFAULT));
            
            // issue request.
    		HttpHost target = new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme());
    		
    		ExecContext context = new ExecContext(target, method, null, opType, 0);
            response = nioclient.DELETE(target, method, context);
            
        } catch (SocketTimeoutException ste) {
            throw new StorageTimeoutException(ste);
        } catch (InterruptedIOException ie) {
            throw new StorageInterruptedException(ie);
        } catch (SwiftException se) {
            String msg = se.getHttpStatusLine().toString();
            throw new StorageException(msg, se);
        } catch (Exception e) {
        	e.printStackTrace();
            throw new StorageException(e);
        } finally {
            if (response != null)
                EntityUtils.consume(response.getEntity());
        }
    }

    @Override
    public void deleteObject(final String opType, String container, String object, Config config) throws StorageException, IOException {
        super.deleteObject(opType, container, object, config);

        HttpResponse response = null;
		if(nioclient == null)
		{
			logger.error("nio client is not initialized yet!");
			return;
		}
        
        try {
        	// construct request.
        	URI uri = URI.create(parms.getStr(STORAGE_URL_KEY, STORAGE_URL_DEFAULT));   
        	method = nioclient.makeHttpDelete(uri.getPath() + "/" + container + "/" + object);
            method.setHeader(X_AUTH_TOKEN, parms.getStr(AUTH_TOKEN_KEY, AUTH_TOKEN_DEFAULT));
            
            // issue request.
    		HttpHost target = new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme());

    		ExecContext context = new ExecContext(target, method, null, opType, 0);
            nioclient.DELETE(target, method, context);
            
        } catch (SocketTimeoutException ste) {
            throw new StorageTimeoutException(ste);
        } catch (InterruptedIOException ie) {
            throw new StorageInterruptedException(ie);
        } catch (SwiftException se) {
            String msg = se.getHttpStatusLine().toString();
            throw new StorageException(msg, se);
        } catch (Exception e) {
        	e.printStackTrace();
            throw new StorageException(e);
        } finally {
            if (response != null)
                EntityUtils.consume(response.getEntity());
        }
        
    }

    @Override
	public void createMetadata(final String opType, String container, String object,
            Map<String, String> map, Config config) {
        super.createMetadata(opType, container, object, map, config);
//        try {
//            client.storeObjectMetadata(container, object, map);
//        } catch (SocketTimeoutException ste) {
//            throw new StorageTimeoutException(ste);
//        } catch (InterruptedIOException ie) {
//            throw new StorageInterruptedException(ie);
//        } catch (SwiftException se) {
//            String msg = se.getHttpStatusLine().toString();
//            throw new StorageException(msg, se);
//        } catch (Exception e) {
//            throw new StorageException(e);
//        }
    }

    @Override
	public Map<String, String> getMetadata(final String opType, String container, String object,
            Config config) {
        return super.getMetadata(opType, container, object, config);
//        try {
//            return client.getObjectMetadata(container, object);
//        } catch (SocketTimeoutException ste) {
//            throw new StorageTimeoutException(ste);
//        } catch (InterruptedIOException ie) {
//            throw new StorageInterruptedException(ie);
//        } catch (SwiftException se) {
//            String msg = se.getHttpStatusLine().toString();
//            throw new StorageException(msg, se);
//        } catch (Exception e) {
//            throw new StorageException(e);
//        }
    }

//    private String encodeURL(String str) {
//        URLCodec codec = new URLCodec();
//        try {
//            return codec.encode(str).replaceAll("\\+", "%20");
//        } catch (EncoderException ee) {
//            return str;
//        }
//    }
    
//    private String getContainerPath(String container) {
//        return parms.getStr(STORAGE_URL_KEY, STORAGE_URL_DEFAULT) + "/" + encodeURL(container);
//    }

//    private String getObjectPath(String container, String object) {
//        return getContainerPath(container) + "/" + encodeURL(object);
//    }
}
