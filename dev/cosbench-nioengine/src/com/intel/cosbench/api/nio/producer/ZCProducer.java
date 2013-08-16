package com.intel.cosbench.api.nio.producer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.nio.ContentEncoder;
import org.apache.http.nio.IOControl;
import org.apache.http.nio.entity.HttpAsyncContentProducer;
import org.apache.http.protocol.HttpContext;

/**
 * One zero-copy producer, which can accept different producer source.
 * 
 * @author ywang19
 *
 * @param <T>
 */
public class ZCProducer<T> implements HttpAsyncContentProducer{

	private final ProducerSource<T> source;
//	private final URI requestURI;
//	protected final ContentType contentType;

	public ZCProducer(final ProducerSource<T> source /* final URI requestURI, */
			/* final ContentType contentType */) throws FileNotFoundException {
		super();
//		Args.notNull(requestURI, "Request URI");
		this.source = source;
//		this.requestURI = requestURI;
//		this.contentType = contentType;
	}
	
	public HttpEntityEnclosingRequest createRequest(final URI requestURI,
			final HttpEntity entity) {
		final HttpPut httpput = new HttpPut(requestURI);
		httpput.setEntity(entity);
		return httpput;
	}

//	public synchronized HttpRequest generateRequest() {
//		final BasicHttpEntity entity = new BasicHttpEntity();
//		entity.setChunked(false);
//		entity.setContentLength(this.source.getContentLength());
//		if (this.contentType != null) {
//			entity.setContentType(this.contentType.toString());
//		}
//		return createRequest(this.requestURI, entity);
//	}

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
		if(this.source != null)
			this.source.resetRequest();
	}

	public synchronized void close() throws IOException {
		if(this.source != null)
			this.source.close();
	}

	public HttpEntity getEntity() {
		if(this.source != null)
			return this.source.getEntity();
		return null;
	}

//	public synchronized HttpHost getTarget() {
//		return URIUtils.extractHost(this.requestURI);
//	}

	public void requestCompleted(final HttpContext context) {
	}

	public void failed(final Exception ex) {
	}

	public boolean isRepeatable() {
		return true;
	}

}
