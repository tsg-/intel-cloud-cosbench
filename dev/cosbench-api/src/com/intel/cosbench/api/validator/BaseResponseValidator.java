package com.intel.cosbench.api.validator;

import java.io.FileNotFoundException;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;

import com.intel.cosbench.api.context.Context;



/**
 * The validator will validate response, each adaptor expects to derive its own response validator to handle different response cases.
 * 
 * @author ywang19
 *
 */
public class BaseResponseValidator extends ResponseValidator {
//	protected volatile Context context;
	
	public BaseResponseValidator() {
//		this.context = new Context();
	}
	
	@Override
 	public boolean validate(final HttpResponse response, Context context) throws Throwable {
//		this.context = context;
		
 		if(response == null)
			throw new Exception("No response");
		
		int statusCode = response.getStatusLine().getStatusCode();
		if (statusCode >= HttpStatus.SC_OK && statusCode < (HttpStatus.SC_OK + 100)) {
	    	  EntityUtils.consume(response.getEntity());
	    	  return true;
		}
	  
		if (statusCode == HttpStatus.SC_NOT_FOUND)
			throw new FileNotFoundException("The target is requesting cannot be found!");

		return false;
	}

//	@Override
//	public Context getResults() {
//		return context;
//	}
    
}
