package com.intel.cosbench.api.context;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.protocol.HTTP;


public class ExecContext extends Context {
	public long timestamp;
	public HttpHost target;
	public HttpRequest request;
	public HttpResponse response;
	public long threadId;
	public String operator;
	public long length;
	public boolean status;
	
	public ExecContext(final HttpHost target,final HttpRequest request,final HttpResponse response, final String operator, final long length) {
		this.timestamp = System.currentTimeMillis();
		this.threadId = Thread.currentThread().getId();
		this.operator = operator;
		this.target = target;
		this.request = request;
		this.response = response;
		this.length = length;
	}
	
	public String getUri() {
		if(request != null) {
			return request.getRequestLine().getUri();
		}else
			return "null";
	}
	
	public String getOpType() {
		return operator;
	}
	
	public long getLength() {
		if(response != null) {
			Header header = response.getFirstHeader(HTTP.CONTENT_LEN);
			if(header != null)
				this.length = Long.parseLong(header.getValue());
		}
		
		return length;			
	}
}
