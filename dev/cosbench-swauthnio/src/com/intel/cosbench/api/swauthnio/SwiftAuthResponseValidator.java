package com.intel.cosbench.api.swauthnio;

import static com.intel.cosbench.client.swauthnio.SwiftAuthConstants.*;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;

import com.intel.cosbench.api.context.ExecContext;
import com.intel.cosbench.api.validator.BaseResponseValidator;
import com.intel.cosbench.client.swauthnio.SwiftAuthClientException;

public class SwiftAuthResponseValidator extends BaseResponseValidator {
	
	public SwiftAuthResponseValidator() {
	}
	
 	@Override
	public boolean validate(final HttpResponse response, ExecContext context) throws Exception {
// 		this.context = context;
 		
		if(response == null)
			throw new Exception("No response");
		
		int statusCode = response.getStatusLine().getStatusCode();
		
		 if ((response.getStatusLine().getStatusCode() >= HttpStatus.SC_OK) &&
	 				(response.getStatusLine().getStatusCode() < (HttpStatus.SC_OK + 100))
	 				) 
		 {
			String authToken = response.getFirstHeader(X_AUTH_TOKEN) != null ? response
			         .getFirstHeader(X_AUTH_TOKEN).getValue() : null;
			String storageURL = response.getFirstHeader(X_STORAGE_URL) != null ? response
			         .getFirstHeader(X_STORAGE_URL).getValue() : null;
			context.put(AUTH_TOKEN_KEY, authToken);
			context.put(STORAGE_URL_KEY, storageURL);

			EntityUtils.consume(response.getEntity()); 
			
			return true;
         }
		
		throw new SwiftAuthClientException(statusCode, "unexpected result from server",
	          response.getAllHeaders(), response.getStatusLine());
	}
}
