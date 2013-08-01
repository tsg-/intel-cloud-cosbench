package com.intel.cosbench.api.validator;

import org.apache.http.HttpResponse;

import com.intel.cosbench.api.context.Context;


/**
 * The validator will validate response, each adaptor expects to derive its own response validator to handle different response cases.
 * 
 * @author ywang19
 *
 */
public abstract class ResponseValidator {

	public abstract boolean validate(final HttpResponse response, Context context) throws Throwable;
	public abstract Context getResults();
    
}
