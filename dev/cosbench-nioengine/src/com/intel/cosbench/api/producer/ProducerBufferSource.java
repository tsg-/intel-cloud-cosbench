package com.intel.cosbench.api.producer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Random;

import org.apache.commons.io.input.NullInputStream;
import org.apache.http.HttpEntity;
import org.apache.http.annotation.NotThreadSafe;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.nio.ContentEncoder;
import org.apache.http.nio.IOControl;


/**
 * A simple self contained, repeatable non-blocking entity that retrieves its
 * content from a byte array.
 * 
 * @since 4.0
 */
@NotThreadSafe
public class ProducerBufferSource extends ProducerSource<ByteBuffer> {

	private ByteBuffer buffer;
	private ReadableByteChannel channel;

	private ProducerRandomInputStream in;

	private final int size;

	public ProducerBufferSource(Random random,final int size) {
		super();
		this.size = size;
//		this.in = in;
		this.buffer = ByteBuffer.allocate(this.size);
		in = new ProducerRandomInputStream(this.size,random,true,false);
	}

	public void produceContent(final ContentEncoder encoder,
			final IOControl ioctrl) throws IOException {

		if (this.channel == null) {
			this.channel = Channels.newChannel(this.in);
		}
		final int i = this.channel.read(this.buffer);
		this.buffer.flip();
		encoder.write(this.buffer);
		final boolean buffering = this.buffer.hasRemaining();
		this.buffer.compact();
		if (i == -1 && !buffering) {
			encoder.complete();
		}
	}

	public HttpEntity getEntity() {
		return new InputStreamEntity(this.in,
				((NullInputStream) this.in).getSize());
	}

	public boolean isRepeatable() {
		return true;
	}

	public void close() throws IOException {
		closeChannel();
	}

	@Override
	public long getContentLength() {
		return this.size;
	}

	@Override
	public void resetRequest() throws IOException {
		closeChannel();
	}

	private void closeChannel() throws IOException {
		final ReadableByteChannel local = this.channel;
		this.channel = null;
		if (local != null) {
			local.close();
		}
	}

	@Override
	public ByteBuffer getSource() {
		return this.buffer;
	}

	@Override
	public boolean isStreaming() {
		return false;
	}

}
