package com.intel.cosbench.api.swiftnio;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;

import com.intel.cosbench.api.validator.ResponseValidator;
import com.intel.cosbench.client.swiftnio.SwiftException;
import com.intel.cosbench.client.swiftnio.SwiftFileNotFoundException;

public class SwiftResponseValidator extends ResponseValidator {

	public SwiftResponseValidator() {
		
	}
	
 	@SuppressWarnings("deprecation")
	public boolean validate(final HttpResponse response, Object context) throws Throwable {
		if(response == null)
			throw new Exception("No response");
		
		int statusCode = response.getStatusLine().getStatusCode();
		if (statusCode == HttpStatus.SC_OK) {
	    	  response.getEntity().consumeContent();
	    	  return true;
		}
		
		if (statusCode == HttpStatus.SC_NOT_FOUND) 
			throw new SwiftFileNotFoundException("object not found", response.getAllHeaders(), response.getStatusLine());
		
		throw new SwiftException("unexpected result from server",
	          response.getAllHeaders(), response.getStatusLine());
	}
}
