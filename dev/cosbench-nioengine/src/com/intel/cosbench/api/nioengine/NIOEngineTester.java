package com.intel.cosbench.api.nioengine;

import java.util.Random;

import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHttpRequest;

import com.intel.cosbench.api.client.NIOClient;
import com.intel.cosbench.api.util.NIOEngineUtil;
import com.intel.cosbench.log.LogFactory;
import com.intel.cosbench.log.LogManager;
import com.intel.cosbench.log.Logger;


public class NIOEngineTester {

	NIOEngine ioengine;
	final static String doc_root = "c:/temp/download/";
	
	public void init()
	{
    	ioengine = new NIOEngine();

    	LogManager manager = LogFactory.createLogManager();
        Logger logger = manager.getLogger();
        	
    	ioengine.init(null,logger);
    	ioengine.startup();
	}
	
	public void fini()
	{
    	
    	ioengine.shutdown();
	}
	
	public void testGET()
	{
    	
    	try
    	{
        	NIOClient client = NIOEngineUtil.newClient(ioengine); // new NIOClient(ioengine.getConnPool(), nioengine);
        	

    		HttpHost target = new HttpHost("127.0.0.1", 8080, "http");
            String sub = "128KB";
        	
            Random rnd = new Random(23);
            
            int range = 10;
            int total = 100;
        	
            for(int i=0; i< total; i++)
            {
	            int idx = (rnd.nextInt(range)+1);
	        	
	        	String path = "/" + sub + "/" + idx + ".txt";
	        	System.out.println("Path=" + path);
	            BasicHttpRequest request = client.makeHttpGet(path);
	
	            System.out.println("[" + (i+1) + "]" + " Start Timestamp=" + System.currentTimeMillis());
            
	        	client.GET(target, request);
	            
	            System.out.println("[" + (i+1) + "]" + " End Timestamp=" + System.currentTimeMillis());
            }
            
            client.await();
    	}catch(Exception ie) {
    		ie.printStackTrace();
    	}

    	
	}
	
	public String makePath(String path)
	{
		return doc_root + "/" + path;
	}
	
	public void testPUT()
	{
    	
    	try
    	{
        	NIOClient client = NIOEngineUtil.newClient(ioengine); //new NIOClient(ioengine.getConnPool());

    		HttpHost target = new HttpHost("127.0.0.1", 8080, "http");
            String sub = "128KB";
        	
            Random rnd = new Random(23);
            
            int range = 10;
            int total = 100;            

            for(int i=0; i< total; i++)
            {
	            int idx = (rnd.nextInt(range)+1);
	        	
	        	String path = "/" + sub + "/" + idx + ".txt";
	        	System.out.println("Path=" + path);
	        	
	        	HttpEntityEnclosingRequest request = client.makeHttpPut(path);
	            
	            System.out.println("[" + (i+1) + "]" + " Start Timestamp=" + System.currentTimeMillis());
            
	        	client.PUT(target, request);
	            
	            System.out.println("[" + (i+1) + "]" + " End Timestamp=" + System.currentTimeMillis());
            }
            
            client.await();
    	}catch(Exception ie) {
    		ie.printStackTrace();
    	}    	
	}
	
	public void testDELETE()
	{
    	
    	try
    	{
        	NIOClient client = NIOEngineUtil.newClient(ioengine); //new NIOClient(ioengine.getConnPool());

    		HttpHost target = new HttpHost("127.0.0.1", 8080, "http");
            String sub = "128KB";
        	
            Random rnd = new Random(23);
            
            int range = 10;
            int total = 10;            

            for(int i=0; i< total; i++)
            {
	            int idx = (rnd.nextInt(range)+1);
	        	
	        	String path = "/" + sub + "/" + idx + ".txt";
	        	System.out.println("Path=" + path);
	        	
	        	BasicHttpRequest request = client.makeHttpDelete(path);
	            
	            System.out.println("[" + (i+1) + "]" + " Start Timestamp=" + System.currentTimeMillis());
            
	        	client.DELETE(target, request);
	            
	            System.out.println("[" + (i+1) + "]" + " End Timestamp=" + System.currentTimeMillis());
            }
            
            client.await();
    	}catch(Exception ie) {
    		ie.printStackTrace();
    	}

    	
	}
	
	
    public static void main(String[] args)
    {
    	NIOEngineTester tester = new NIOEngineTester();
    	tester.init();
    	
//    	tester.testGET();
//    	
//    	tester.testPUT();
    	
    	tester.testDELETE();
    	
    	tester.fini();
    	
    }
    
}
