package com.intel.cosbench.api.nioengine;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequest;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.ContentEncoder;
import org.apache.http.nio.IOControl;

/**
 * One zero-copy producer, which can accept different producer source.
 * 
 * @author ywang19
 *
 * @param <T>
 */
public class ZCProducer<T> extends BaseZCProducer {

	private final ProducerSource<T> source;
	private final URI requestURI;

	public ZCProducer(final ProducerSource<T> source, final URI requestURI,
			final ContentType contentType) throws FileNotFoundException {
		super(requestURI, contentType);
		this.source = source;
		this.requestURI = requestURI;
	}

	// @Override
	public HttpEntityEnclosingRequest createRequest(final URI requestURI,
			final HttpEntity entity) {
		final HttpPut httpput = new HttpPut(requestURI);
		httpput.setEntity(entity);
		return httpput;
	}

	@Override
	public synchronized HttpRequest generateRequest() {
		final BasicHttpEntity entity = new BasicHttpEntity();
		entity.setChunked(false);
		entity.setContentLength(this.source.getContentLength());
		if (this.contentType != null) {
			entity.setContentType(this.contentType.toString());
		}
		return createRequest(this.requestURI, entity);
	}

	@Override
	public synchronized void produceContent(final ContentEncoder encoder,
			final IOControl ioctrl) throws IOException {
		if (this.source != null) {
			this.source.produceContent(encoder, ioctrl);
			if (encoder.isCompleted()) {
				this.source.close();
			}
		}
	}

	public synchronized void resetRequest() throws IOException {
		this.source.resetRequest();
	}

	public synchronized void close() throws IOException {
		this.source.close();
	}

	public HttpEntity getEntity() {
		return this.source.getEntity();
	}
}
