package com.intel.cosbench.api.nioengine;

import org.apache.http.HttpResponse;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.HttpAsyncContentProducer;

abstract class ProducerSource<T> implements HttpAsyncContentProducer {
        protected HttpResponse response;
        protected ContentType contentType;
        protected T source;
	    
		public ProducerSource(T source) {
	        if (source == null) {
	            throw new IllegalArgumentException("File may nor be null");
	        }
	        this.source = source;
		}
		
		ContentType getContentType() {
			return this.contentType; 
		}
		
		T getSource() {
			return this.source;
		}
	
		public boolean isRepeatable() {
			return true;
		}

		public boolean isStreaming() {
			return false;
		}
}