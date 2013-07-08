package com.intel.cosbench.api.nioengine;

import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.ContentEncoder;
import org.apache.http.nio.IOControl;
import org.apache.http.nio.entity.HttpAsyncContentProducer;

/**
 * This class abstracts the functionalities necessary for  data producer.
 * 
 * @author ywang19
 *
 * @param <T>
 */
public abstract class ProducerSource<T> implements HttpAsyncContentProducer {
	protected HttpResponse response;
	protected ContentType contentType;

	public ProducerSource() {
		/* empty */
	}

	ContentType getContentType() {
		return this.contentType;
	}

	public abstract void produceContent(final ContentEncoder encoder,
			final IOControl ioctrl) throws IOException;

	public abstract long getContentLength();

	public abstract T getSource();

	public boolean isRepeatable() {
		return true;
	}
	public abstract HttpEntity getEntity();

	public abstract boolean isStreaming();

	public abstract void resetRequest() throws IOException;

	public abstract void close() throws IOException;

}