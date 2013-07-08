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
import java.util.Map;
import java.util.Random;

import org.apache.http.HttpHost;
import org.apache.http.message.BasicHttpRequest;

import com.intel.cosbench.api.nio.client.NIOClient;
import com.intel.cosbench.api.context.AuthContext;
import com.intel.cosbench.api.nio.engine.NIOEngine;
import com.intel.cosbench.api.nio.util.*;
import com.intel.cosbench.api.storage.*;
import com.intel.cosbench.client.http.HttpClientUtil;
import com.intel.cosbench.client.swiftnio.*;
import com.intel.cosbench.config.Config;
import com.intel.cosbench.log.LogFactory;
import com.intel.cosbench.log.LogManager;
import com.intel.cosbench.log.Logger;

/**
 * This class encapsulates a Swift implementation for Storage API.
 * 
 * @author ywang19, qzheng7
 * 
 */
class SwiftNioStorage extends NoneStorage {

    private SwiftClient client;

    /* configurations */
    private int timeout; // connection and socket timeout
    
    /* current operation */
    private volatile BasicHttpRequest method;
    
    /* user context */
    private String authToken;
    private String storageURL;

    private NIOClient nioclient;
//    private StatsCallback callback;

    
    public SwiftNioStorage() {
    	
    }
    
    public void init(NIOClient client /*, FutureCallback callback */) {
//        this.connPool = connPool;
    	this.nioclient = client;
//    	this.callback = callback;
    }

    @Override
    public void init(Config config, Logger logger) {
        super.init(config, logger);

        timeout = config.getInt(CONN_TIMEOUT_KEY, CONN_TIMEOUT_DEFAULT);

        parms.put(CONN_TIMEOUT_KEY, timeout);

        
        logger.debug("using storage config: {}", parms);

        logger.debug("swift client has been initialized");
    }

    @Override
    public void setAuthContext(AuthContext info) {
        super.setAuthContext(info);
//        authToken = info.getStr(AUTH_TOKEN_KEY);
//        storageURL = info.getStr(STORAGE_URL_KEY);
//        try {
//            client.init(authToken, storageURL);
//        } catch (Exception e) {
//            throw new StorageException(e);
//        }
        logger.debug("using auth token: {}, storage url: {}", authToken, storageURL);
    }

    @Override
    public void dispose() {
        super.dispose();
        client.dispose();
    }

    @Override
    public void abort() {
        super.abort();
        client.abort();
    }

    public static void testGetObject()
    {
       	NIOEngine ioengine = new NIOEngine();

    	LogManager manager = LogFactory.createLogManager();
        Logger logger = manager.getLogger();
        	
    	ioengine.init(null,logger);
    	ioengine.startup();
    	
    	NIOClient ioclient = NIOEngineUtil.newClient(ioengine);
    	
    	SwiftNioStorage storage = new SwiftNioStorage();
    	storage.init(ioclient);
    	
    	try
    	{
//            HttpHost target = new HttpHost("127.0.0.1", 8080, "http");
            String container = "128KB";
        	
            Random rnd = new Random(23);
            
            int range = 10;

//            CountDownLatch latch = new CountDownLatch(ioengine.getConcurrency());
            
            for(int i=0; i< 100; i++)
            {
	            int idx = (rnd.nextInt(range)+1);
	        	
	            String object = idx + ".txt";
	        	String path = "/" + container + "/" + idx + ".txt";
	        	System.out.println("Path=" + path);
//	            BasicHttpRequest request = new BasicHttpRequest("GET", path);
	
	            System.out.println("[" + (i+1) + "]" + " Start Timestamp=" + System.currentTimeMillis());
	            
//	            ioengine.issueRequest(target , request, latch);
	            
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
        InputStream stream;
        try {      
        	
        	// construct request.
            method = nioclient.makeHttpGet("/" + container + "/" + object);
            method.setHeader(X_AUTH_TOKEN, authToken);           
            
            // issue request.
    		HttpHost target = new HttpHost("127.0.0.1", 8080, "http");
//        	
//        	String path = "/" + container + "/" + object;
//        	System.out.println("Path=" + path);
//            BasicHttpRequest request = new BasicHttpRequest("GET", path);
            
//            CountDownLatch latch = new CountDownLatch(1);
            nioclient.GET(target, method);
            
//            latch.await();
            
            // check response.
            // in FutureCallback.
            
//            if (response.getStatusCode() == SC_OK)
//                return response.getResponseBodyAsStream();
//            response.consumeResposeBody();
//            
//            if (response.getStatusCode() == SC_NOT_FOUND)
//                throw new SwiftFileNotFoundException("object not found: "
//                        + container + "/" + object, response.getResponseHeaders(),
//                        response.getStatusLine());
//            throw new SwiftException("unexpected result from server",
//                    response.getResponseHeaders(), response.getStatusLine());
            
            return null;
            
        } catch (SocketTimeoutException ste) {
            throw new StorageTimeoutException(ste);
        } catch (InterruptedIOException ie) {
            throw new StorageInterruptedException(ie);
        } catch (SwiftException se) {
            String msg = se.getHttpStatusLine().toString();
            throw new StorageException(msg, se);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void createContainer(String container, Config config) {
        super.createContainer(container, config);
        try {
            if (!client.containerExists(container))
                client.createContainer(container);
        } catch (SocketTimeoutException ste) {
            throw new StorageTimeoutException(ste);
        } catch (InterruptedIOException ie) {
            throw new StorageInterruptedException(ie);
        } catch (SwiftException se) {
            String msg = se.getHttpStatusLine().toString();
            throw new StorageException(msg, se);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Deprecated
    public void createObject(String container, String object, byte[] data,
            Config config) {
        super.createObject(container, object, data, config);
        try {
            client.storeObject(container, object, data);
        } catch (SocketTimeoutException ste) {
            throw new StorageTimeoutException(ste);
        } catch (InterruptedIOException ie) {
            throw new StorageInterruptedException(ie);
        } catch (SwiftException se) {
            String msg = se.getHttpStatusLine().toString();
            throw new StorageException(msg, se);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void createObject(String container, String object, InputStream data,
            long length, Config config) {
        super.createObject(container, object, data, length, config);
        try {
            client.storeStreamedObject(container, object, data, length);
        } catch (SocketTimeoutException ste) {
            throw new StorageTimeoutException(ste);
        } catch (InterruptedIOException ie) {
            throw new StorageInterruptedException(ie);
        } catch (SwiftException se) {
            String msg = se.getHttpStatusLine().toString();
            throw new StorageException(msg, se);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void deleteContainer(String container, Config config) {
        super.deleteContainer(container, config);
        try {
            if (client.containerExists(container))
                client.deleteContainer(container);
        } catch (SocketTimeoutException ste) {
            throw new StorageTimeoutException(ste);
        } catch (InterruptedIOException ie) {
            throw new StorageInterruptedException(ie);
        } catch (SwiftException se) {
            String msg = se.getHttpStatusLine().toString();
            throw new StorageException(msg, se);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void deleteObject(String container, String object, Config config) {
        super.deleteObject(container, object, config);
        try {
            client.deleteObject(container, object);
        } catch (SocketTimeoutException ste) {
            throw new StorageTimeoutException(ste);
        } catch (InterruptedIOException ie) {
            throw new StorageInterruptedException(ie);
        } catch (SwiftException se) {
            String msg = se.getHttpStatusLine().toString();
            throw new StorageException(msg, se);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    protected void createMetadata(String container, String object,
            Map<String, String> map, Config config) {
        super.createMetadata(container, object, map, config);
        try {
            client.storeObjectMetadata(container, object, map);
        } catch (SocketTimeoutException ste) {
            throw new StorageTimeoutException(ste);
        } catch (InterruptedIOException ie) {
            throw new StorageInterruptedException(ie);
        } catch (SwiftException se) {
            String msg = se.getHttpStatusLine().toString();
            throw new StorageException(msg, se);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    protected Map<String, String> getMetadata(String container, String object,
            Config config) {
        super.getMetadata(container, object, config);
        try {
            return client.getObjectMetadata(container, object);
        } catch (SocketTimeoutException ste) {
            throw new StorageTimeoutException(ste);
        } catch (InterruptedIOException ie) {
            throw new StorageInterruptedException(ie);
        } catch (SwiftException se) {
            String msg = se.getHttpStatusLine().toString();
            throw new StorageException(msg, se);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    private String getContainerPath(String container) {
        return storageURL + "/" + HttpClientUtil.encodeURL(container);
    }

    private String getObjectPath(String container, String object) {
        return getContainerPath(container) + "/" + HttpClientUtil.encodeURL(object);
    }
}
