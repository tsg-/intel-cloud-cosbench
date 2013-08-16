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

package com.intel.cosbench.api.nio.engine;

import static com.intel.cosbench.api.nio.engine.NIOEngineConstants.*;

import java.io.IOException;
import java.io.InterruptedIOException;

import org.apache.http.config.ConnectionConfig;
import org.apache.http.impl.nio.DefaultHttpClientIODispatch;
import org.apache.http.impl.nio.pool.BasicNIOConnPool;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.protocol.HttpAsyncRequestExecutor;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOEventDispatch;

import com.intel.cosbench.config.Config;
import com.intel.cosbench.log.Logger;
//import com.intel.cosbench.api.client.NIOClient;
import com.intel.cosbench.api.context.*;
import com.intel.cosbench.api.ioengine.*;
import com.intel.cosbench.api.nio.client.NIOClient;


/**
 * This class encapsulates a NIO Engine implementation for the IOEngine API.
 * 
 * @IOEngineor ywang19
 * 
 */

/* below is for httpcore 4.3-beta2 */

/**
 * This class encapsulates basic operations need to setup NIO engine.
 * 
 * @author ywang19
 *
 */
public class NIOEngine extends NoneIOEngine {

	private int channels = IOENGINE_CHANNELS_DEFAULT;		// the number of working channel.
	private int concurrency = IOENGINE_CONCURRENCY_DEFAULT; 	// the queue or pool size in max.

	private ConnectingIOReactor ioReactor;
	private IOEventDispatch ioEventDispatch;
	private BasicNIOConnPool connPool;
	

	public BasicNIOConnPool getConnPool() {
		return connPool;
	}

	public int getChannels() {
		return channels;
	}

	public void setChannels(int channels) {
		this.channels = channels;
	}

	public int getConcurrency() {
		return concurrency;
	}

	public void setConcurrency(int concurrency) {
		this.concurrency = concurrency;
	}

   
    public NIOEngine() {
    }

    @Override
	public NIOClient newClient() {
    	return new NIOClient(getConnPool());
    }
    
    public NIOClient newClient(int concurrency) {
    	return new NIOClient(getConnPool(), concurrency);
    }
        
    @Override
    public boolean init(Config config, Logger logger) {
        super.init(config, logger);
        
        //@TODO
        if(config != null)
        {
			channels = config.getInt(IOENGINE_CHANNELS_KEY, IOENGINE_CHANNELS_DEFAULT);
			concurrency = config.getInt(IOENGINE_CONCURRENCY_KEY, IOENGINE_CONCURRENCY_DEFAULT);
        } else {
	          channels = 2;	// how many io channel reactors will be used to serve i/o.
	          concurrency = 4;	// how many outstanding io can support.
        }

        parms.put(IOENGINE_CHANNELS_KEY, channels);
        parms.put(IOENGINE_CONCURRENCY_KEY, concurrency);
                
        logger.info("using IOEngine config: {}", parms);
        
        try
        {
	        HttpAsyncRequestExecutor protocolHandler = new HttpAsyncRequestExecutor();
	        ioEventDispatch = new DefaultHttpClientIODispatch(protocolHandler, ConnectionConfig.DEFAULT);
	        ioReactor = new DefaultConnectingIOReactor(IOReactorConfig.custom()
	        		.setIoThreadCount(channels)
	        		.build(), 
	        		null);
	        connPool = new BasicNIOConnPool(ioReactor, ConnectionConfig.DEFAULT);
	        connPool.setDefaultMaxPerRoute(concurrency);
	        connPool.setMaxTotal(concurrency);
        }catch(Exception e) {
            logger.debug("NIOEngine is failed to initialize");
        	e.printStackTrace();
        	
        	return false;
        }

        logger.debug("NIOEngine has been initialized");
        
        
        return true;
    }

    @Override
    public void dispose() {
        super.dispose();
    }

    @Override
    public boolean shutdown() throws IOEngineException {
    	
    	try {    		
    		ioReactor.shutdown();
    	}catch(IOException e) {
    		logger.error("Failed to shut down I/O Reactor.");
    		
    		throw new IOEngineException(e);
    	}
    	
    	return true;
    }
    
    @Override
    public IOEngineContext startup() throws IOEngineException {
        super.startup();
        
        try {
        	Thread ioThread = new Thread(new Runnable() {
                public void run() {
                    try {
                        ioReactor.execute(ioEventDispatch);
                    } catch (InterruptedIOException ex) {
                        System.err.println("Interrupted");
                    } catch (IOException e) {
                        System.err.println("I/O error: " + e.getMessage());
                    }
                    System.out.println("Shutdown");
                }
            });
            ioThread.start();            
        	
        } catch (Exception e) {
        	logger.error(e.getMessage());
        	
        	throw new IOEngineException(e);
        }
        
        return createContext();
    }

    private IOEngineContext createContext() {
        IOEngineContext context = new IOEngineContext();
        return context;
    }
    


}

