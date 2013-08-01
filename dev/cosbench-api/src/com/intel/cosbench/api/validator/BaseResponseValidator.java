package com.intel.cosbench.api.validator;

import java.io.FileNotFoundException;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;

import com.intel.cosbench.api.context.Context;



/**
 * The validator will validate response, each adaptor expects to derive its own response validator to handle different response cases.
 * 
 * @author ywang19
 *
 */
public class BaseResponseValidator extends ResponseValidator {
	protected Context context;
	
	public BaseResponseValidator() {
	}
	
 	public boolean validate(final HttpResponse response, Context context) throws Throwable {
		this.context = context;
		
 		if(response == null)
			throw new Exception("No response");
		
		int statusCode = response.getStatusLine().getStatusCode();
		if (statusCode == HttpStatus.SC_OK)
			return true;
	  
		if (statusCode == HttpStatus.SC_NOT_FOUND)
			throw new FileNotFoundException("The target is requesting cannot be found!");

		return false;
	}

	@Override
	public Context getResults() {
		return context;
	}
    
}
