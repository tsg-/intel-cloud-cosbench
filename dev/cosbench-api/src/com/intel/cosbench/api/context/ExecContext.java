package com.intel.cosbench.api.context;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.protocol.HTTP;


public class ExecContext implements StatsContext {
	public long timestamp;
	public HttpHost target;
	public HttpRequest request;
	public HttpResponse response;
	public boolean composited;
//	public long size;
	
	public ExecContext(final HttpHost target,final HttpRequest request,final HttpResponse response) {
		this.timestamp = System.currentTimeMillis();
		this.target = target;
		this.request = request;
		this.response = response;
	}
	
	public String getUri() {
		if(request != null) {
			return request.getRequestLine().getUri();
		}else
			return "null";
	}
	
	public long getLength() {
		if(response != null) {
			Header header = response.getFirstHeader(HTTP.CONTENT_LEN);
			if(header != null) 
				return Long.parseLong(header.getValue());				
		}
		
		return 0;			
	}
}
