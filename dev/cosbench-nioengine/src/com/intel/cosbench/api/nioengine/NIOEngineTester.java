package com.intel.cosbench.api.nioengine;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.apache.http.HttpHost;
import org.apache.http.message.BasicHttpRequest;

import com.intel.cosbench.log.LogFactory;
import com.intel.cosbench.log.LogManager;
import com.intel.cosbench.log.Logger;

public class NIOEngineTester {

    public static void main(String[] args)
    {
    	NIOEngine ioengine = new NIOEngine();

    	LogManager manager = LogFactory.createLogManager();
        Logger logger = manager.getLogger();
        	
    	ioengine.init(null,logger);
    	ioengine.startup();
    	
    	try
    	{
        	NIOClient client = new NIOClient(ioengine.getConnPool());

    		HttpHost target = new HttpHost("127.0.0.1", 8080, "http");
            String sub = "128KB";
        	
            Random rnd = new Random(23);
            
            int range = 10;

//            CountDownLatch latch = new CountDownLatch(ioengine.getConcurrency());
            
            for(int i=0; i< 100; i++)
            {
	            int idx = (rnd.nextInt(range)+1);
	        	
	        	String path = "/" + sub + "/" + idx + ".txt";
	        	System.out.println("Path=" + path);
	            BasicHttpRequest request = new BasicHttpRequest("GET", path);
	
	            System.out.println("[" + (i+1) + "]" + " Start Timestamp=" + System.currentTimeMillis());
            
	        	client.issueRequest(target, request);
	            
	            System.out.println("[" + (i+1) + "]" + " End Timestamp=" + System.currentTimeMillis());
            }
            
            client.await();
    	}catch(Exception ie) {
    		ie.printStackTrace();
    	}
    	
    	ioengine.shutdown();
    	
    }
    
}
