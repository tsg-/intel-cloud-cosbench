package com.intel.cosbench.api.context;

import org.apache.http.Header;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.protocol.HTTP;


public class ExecContext extends Context {
	public long timestamp;
	public HttpHost target;
	private HttpRequest request;
	private volatile HttpResponse response;

	public long threadId;
	public String operator;
	private long length;	// the length for data uploading or download.
	private boolean status;	


	public ExecContext(final HttpHost target,final HttpRequest request,final HttpResponse response, final String operator, final long length) {
		this.timestamp = System.currentTimeMillis();
		this.threadId = Thread.currentThread().getId();
		this.operator = operator;
		this.target = target;
		this.setRequest(request);
		this.setResponse(response);
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
		return length;
	}
	
	public boolean getStatus() {
		return status;
	}

	public void setStatus(boolean status) {
		this.status = status;
	}	
	
	public HttpResponse getResponse() {
		return response;
	}

	public void setResponse(HttpResponse response) {
		this.response = response;
		
		if(this.response != null) {
			Header header = this.response.getFirstHeader(HTTP.CONTENT_LEN);
			if(header != null) {
				length += Long.parseLong(header.getValue());
				System.out.println("Response length = " + (this.response.getEntity() != null? this.response.getEntity().getContentLength(): 0));
			}
		}
	}
	
	public HttpRequest getRequest() {
		return request;
	}

	public void setRequest(HttpRequest request) {
		this.request = request;
	}
}
