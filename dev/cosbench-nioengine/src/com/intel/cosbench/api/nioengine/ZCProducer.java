package com.intel.cosbench.api.nioengine;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URI;

import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;

public class ZCProducer<T> extends BaseZCProducer {

    private final ProducerSource<T> source;
    private HttpResponse response;
    
    public ZCProducer(final ProducerSource<T> source,
            final URI requestURI,
            final File content,
            final ContentType contentType) throws FileNotFoundException {
        super(requestURI, content, contentType);
        this.source = source;
    }

//    @Override
    public HttpEntityEnclosingRequest createRequest(final URI requestURI, final HttpEntity entity) {
        final HttpPut httpput = new HttpPut(requestURI);
        httpput.setEntity(entity);
        return httpput;
    }

}
