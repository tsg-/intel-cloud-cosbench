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
import static org.apache.http.HttpStatus.SC_ACCEPTED;
import static org.apache.http.HttpStatus.SC_CREATED;
import static org.apache.http.HttpStatus.SC_NOT_FOUND;

import java.io.*;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.Map;
import java.util.Random;

import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.message.BasicHttpRequest;

import com.intel.cosbench.api.validator.*;
import com.intel.cosbench.api.nio.client.NIOClient;

import com.intel.cosbench.api.context.Context;
import com.intel.cosbench.api.nio.engine.NIOEngine;
import com.intel.cosbench.api.stats.BaseStatsCollector;
import com.intel.cosbench.api.stats.StatsCollector;
import com.intel.cosbench.api.storage.*;
import com.intel.cosbench.client.http.HttpClientUtil;
//import com.intel.cosbench.client.http.HttpClientUtil;
import com.intel.cosbench.client.swiftnio.*;
import com.intel.cosbench.config.Config;
import com.intel.cosbench.log.LogFactory;
import com.intel.cosbench.log.LogManager;
import com.intel.cosbench.log.Logger;
import com.sun.xml.internal.messaging.saaj.util.ByteInputStream;

/**
 * This class encapsulates a Swift implementation for Storage API.
 * 
 * @author ywang19
 * 
 */
class SwiftNioStorage extends NoneStorage {
	/* configurations */
    private int timeout; // connection and socket timeout
    
    /* current operation */
    private volatile HttpRequest method;
    
    /* user context */
    private volatile String authToken;
    private volatile String storageURL;

    private NIOClient nioclient;
    

    @Override
    public void init(Config config, Logger logger) {
        super.init(config, logger);
        
//        timeout = config.getInt(CONN_TIMEOUT_KEY, CONN_TIMEOUT_DEFAULT);
//        parms.put(CONN_TIMEOUT_KEY, timeout);
        logger.debug("using storage config: {}", parms);

        if(ioengine != null) {
        	nioclient = (NIOClient)ioengine.newClient();
        	nioclient.setValidator(new SwiftResponseValidator());
            logger.info("swift client has been initialized");
        }else {
            logger.error("swift i/o engine is not correctly initialized, please check it first.");
        }

    }
    
    @Override
    public void initCollector(StatsCollector collector) {
    	this.collector = collector;
		if(nioclient != null)
			nioclient.setCollector(collector);
    }

    @Override
    public void initValidator(ResponseValidator validator) {
    	this.validator = validator;
		if(nioclient != null)
			nioclient.setValidator(validator);
    }
    
	public void setValidator(ResponseValidator validator) {
		if(nioclient != null)
			nioclient.setValidator(validator);
	}

	public void setCollector(StatsCollector collector) {
		if(nioclient != null)
			nioclient.setCollector(collector);
	}

	@Override
    public synchronized void setAuthContext(Context info) {
        super.setAuthContext(info);
        if(info != null) {
	        authToken = info.getStr(AUTH_TOKEN_KEY, AUTH_TOKEN_DEFAULT);
	        storageURL = info.getStr(STORAGE_URL_KEY, STORAGE_URL_DEFAULT);
	        
	        parms.put(AUTH_TOKEN_KEY, authToken);
	        parms.put(STORAGE_URL_KEY, storageURL);
        }

        logger.debug("using auth token: {}, storage url: {}", authToken, storageURL);
    }

    @Override
    public void dispose() {
        super.dispose();
//        client.dispose();
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
	            
	            storage.setAuthContext(new Context());
	            storage.getObject(container, object, null);
	            
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
    	testGetObject();
    	
    }
    
    @Override
    public InputStream getObject(String container, String object, Config config) {
        super.getObject(container, object, config);
        InputStream stream = new ByteInputStream();

		if(nioclient == null)
		{
			System.err.println("nio client is not initialized yet!");
			return stream;
		}

        try {              	
        	// construct request.
        	System.out.println("Storage URL = " + storageURL);
        	URI uri = URI.create(storageURL);        	
        	method = nioclient.makeHttpGet(uri.getPath() + "/" + container + "/" + object);
        	method.setHeader(X_AUTH_TOKEN, authToken);              	
            // issue request.
    		HttpHost target = new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme());
    		
            nioclient.GET(target, method);
            
            System.out.println("Request is issued!");
            
            return stream;
            
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
        }
    }

    @Override
    public void createContainer(String container, Config config) {
        super.createContainer(container, config);
//        try {
//            if (!client.containerExists(container))
//                client.createContainer(container);
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

    @Deprecated
    public void createObject(String container, String object, byte[] data,
            Config config) {
        super.createObject(container, object, data, config);
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
    public void createObject(String container, String object, InputStream data,
            long length, Config config) {
        super.createObject(container, object, data, length, config);
		if(nioclient == null)
		{
			System.err.println("nio client is not initialized yet!");
			return;
		}
        
        try {
        	// construct request.
        	System.out.println("Storage URL = " + storageURL);
        	URI uri = URI.create(storageURL);   
        	HttpEntityEnclosingRequest method = nioclient.makeHttpPut(uri.getPath() + "/" + container + "/" + object);
        	this.method = method;
            method.setHeader(X_AUTH_TOKEN, authToken);
            InputStreamEntity entity = new InputStreamEntity(data, length);
            if (length < 0)
                entity.setChunked(true);
            else
                entity.setChunked(false);
            entity.setContentType("application/octet-stream");
            method.setEntity(entity);
            
            // issue request.
    		HttpHost target = new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme());
    		
            nioclient.PUT(target, method);
            
            System.out.println("Request is issued!");
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
        }
    }

    @Override
    public void deleteContainer(String container, Config config) {
        super.deleteContainer(container, config);
//        try {
//            if (client.containerExists(container))
//                client.deleteContainer(container);
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
    public void deleteObject(String container, String object, Config config) {
        super.deleteObject(container, object, config);
//        try {
//            client.deleteObject(container, object);
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
    protected void createMetadata(String container, String object,
            Map<String, String> map, Config config) {
        super.createMetadata(container, object, map, config);
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
    protected Map<String, String> getMetadata(String container, String object,
            Config config) {
        return super.getMetadata(container, object, config);
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

    private String getContainerPath(String container) {
        return storageURL + "/" + HttpClientUtil.encodeURL(container);
    }

    private String getObjectPath(String container, String object) {
        return getContainerPath(container) + "/" + HttpClientUtil.encodeURL(object);
    }
}
