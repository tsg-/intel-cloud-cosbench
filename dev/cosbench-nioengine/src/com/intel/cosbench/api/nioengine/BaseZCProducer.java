/*
 * ====================================================================
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 *
 */
package com.intel.cosbench.api.nioengine;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIUtils;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.ContentEncoder;
import org.apache.http.nio.IOControl;
import org.apache.http.nio.entity.HttpAsyncContentProducer;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.Args;

public abstract class BaseZCProducer implements HttpAsyncContentProducer {

	private final URI requestURI;
	protected final ContentType contentType;


	public BaseZCProducer(final URI requestURI, final ContentType contentType)
			throws FileNotFoundException {
		super();
		Args.notNull(requestURI, "Request URI");
		this.requestURI = requestURI;
		this.contentType = contentType;
	}

	public HttpEntityEnclosingRequest createRequest(final URI requestURI,
			final HttpEntity entity) {
		final HttpPut httpput = new HttpPut(requestURI);
		httpput.setEntity(entity);
		return httpput;
	}

	public abstract HttpRequest generateRequest();

	public synchronized HttpHost getTarget() {
		return URIUtils.extractHost(this.requestURI);
	}

	public abstract void produceContent(final ContentEncoder encoder,
			final IOControl ioctrl) throws IOException;

	public void requestCompleted(final HttpContext context) {
	}

	public void failed(final Exception ex) {
	}

	public boolean isRepeatable() {
		return true;
	}

	public abstract void resetRequest() throws IOException;

	public abstract void close() throws IOException;

	public abstract HttpEntity getEntity();
}
