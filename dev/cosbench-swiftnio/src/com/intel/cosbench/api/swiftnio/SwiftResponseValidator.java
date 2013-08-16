package com.intel.cosbench.api.swiftnio;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;

import com.intel.cosbench.api.context.ExecContext;
import com.intel.cosbench.api.validator.BaseResponseValidator;
import com.intel.cosbench.client.swiftnio.SwiftException;
import com.intel.cosbench.client.swiftnio.SwiftFileNotFoundException;

public class SwiftResponseValidator extends BaseResponseValidator {
	
	public SwiftResponseValidator() {
		
	}
	
 	@Override
	public boolean validate(final HttpResponse response, ExecContext context) throws Exception {
		if(response == null)
			throw new Exception("No response");
		
		int statusCode = response.getStatusLine().getStatusCode();
		if ((statusCode >= HttpStatus.SC_OK && statusCode < (HttpStatus.SC_OK + 100))) {
	    	  EntityUtils.consume(response.getEntity());
	    	  return true;
		}
        
		if (statusCode == HttpStatus.SC_NOT_FOUND) 
			throw new SwiftFileNotFoundException(context.request.getRequestLine().getUri() + " can not be found.", response.getAllHeaders(), response.getStatusLine());
		
		throw new SwiftException("unexpected result from server: " + response.getStatusLine(),
	          response.getAllHeaders(), response.getStatusLine());
	}
}
