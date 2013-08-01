package com.intel.cosbench.api.nio.engine;

import java.util.Random;

import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHttpRequest;

import com.intel.cosbench.api.nio.client.NIOClient;
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
    	NIOClient client = ioengine.newClient(); // new NIOClient(ioengine.getConnPool(), nioengine);
    	
		HttpHost target = new HttpHost("127.0.0.1", 8080, "http");
        String sub = "128KB";
    	
        Random rnd = new Random(23);
        
        int range = 10;
        int total = 20;
    	
        for(int i=0; i< total; i++)
        {
            int idx = (rnd.nextInt(range)+1);
        	
        	String path = "/" + sub + "/" + idx + ".txt";
        	System.out.println("Path=" + path);
        	
        	try {
        		BasicHttpRequest request = client.makeHttpGet(path);
//	            System.out.println("[" + (i+1) + "]" + " Start Timestamp=" + System.currentTimeMillis());
        		client.GET(target, request);
//	            System.out.println("[" + (i+1) + "]" + " End Timestamp=" + System.currentTimeMillis());
        	}catch(Exception ie) {
        		ie.printStackTrace();
        	}
        }
        
        try {
        	client.await();
        }catch(InterruptedException ex) {
        	System.out.println("Latch is interrupted.");
        }
	}
	
	public String makePath(String path)
	{
		return doc_root + "/" + path;
	}
	
	public void testPUT()
	{

        	NIOClient client = ioengine.newClient(); //new NIOClient(ioengine.getConnPool());

    		HttpHost target = new HttpHost("127.0.0.1", 8080, "http");
            String sub = "128KB";
        	
            Random rnd = new Random(23);
            
            int range = 10;
            int total = 20;            

            for(int i=0; i< total; i++)
            {
	            int idx = (rnd.nextInt(range)+1);
	        	
	        	String path = "/" + sub + "/" + idx + ".txt";
	        	System.out.println("Path=" + path);
	        	
	        	
	        	try
	        	{
	        		HttpEntityEnclosingRequest request = client.makeHttpPut(path);
//	            System.out.println("[" + (i+1) + "]" + " Start Timestamp=" + System.currentTimeMillis());
	        		client.PUT(target, request);
//	            System.out.println("[" + (i+1) + "]" + " End Timestamp=" + System.currentTimeMillis());
	        	}catch(Exception ie) {
	        		ie.printStackTrace();
	        	}    	
            }
            
            try {
            	client.await();
            }catch(InterruptedException ex) {
            	System.out.println("Latch is interrupted.");
            }

	}
	
	public void testDELETE()
	{

        	NIOClient client = ioengine.newClient(); //new NIOClient(ioengine.getConnPool());

    		HttpHost target = new HttpHost("127.0.0.1", 8080, "http");
            String sub = "128KB";
        	
            Random rnd = new Random(1023);
            
            int range = 10;
            int total = 10;            

            for(int i=0; i< total; i++)
            {
	            int idx = (rnd.nextInt(range)+1);
	        	
	        	String path = "/" + sub + "/" + idx + ".txt";
	        	System.out.println("Path=" + path);
	        	
	        	
	        	try
	        	{
	        		BasicHttpRequest request = client.makeHttpDelete(path);	            
//	            System.out.println("[" + (i+1) + "]" + " Start Timestamp=" + System.currentTimeMillis());            
	        		client.DELETE(target, request);	            
//	            System.out.println("[" + (i+1) + "]" + " End Timestamp=" + System.currentTimeMillis());
	        	}catch(Exception ie) {
	        		ie.printStackTrace();
	        	}
            }
            
            try {
            	client.await();
            }catch(InterruptedException ex) {
            	System.out.println("Latch is interrupted.");
            }    	
	}
	
	public void pause(long ms) {
		try {
			Thread.sleep(ms);
		}catch(InterruptedException e) {
			e.printStackTrace();
		}
	}
	
    public static void main(String[] args)
    {
    	NIOEngineTester tester = new NIOEngineTester();
    	tester.init();
    	
//    	tester.testGET();
    	
    	tester.testPUT();
//    	
//    	tester.testDELETE();
    	
    	tester.pause(1000);
    	
    	
    	tester.fini();
    	
    }
    
}
