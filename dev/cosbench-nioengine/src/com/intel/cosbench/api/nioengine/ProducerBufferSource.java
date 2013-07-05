package com.intel.cosbench.api.nioengine;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.http.annotation.NotThreadSafe;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.ContentEncoder;
import org.apache.http.nio.IOControl;
import org.apache.http.nio.entity.HttpAsyncContentProducer;
import org.apache.http.util.Args;

/**
 * A simple self contained, repeatable non-blocking entity that retrieves its
 * content from a byte array.
 * 
 * @since 4.0
 */
@NotThreadSafe
public class ProducerBufferSource implements HttpAsyncContentProducer {

	private final int size;
	private byte[] b;
	private ByteBuffer buf;
	private int len;
	private Random random;

	public ProducerBufferSource(Random random, final int size, final ContentType contentType) {
		super();
		Args.notNull(random, "random");
		Args.notNull(size, "byte random size");
		this.size = size;
		this.random = random;
	}

	public void produceContent(final ContentEncoder encoder,
			final IOControl ioctrl) throws IOException {
		gengerateRandomByte();
		encoder.write(this.buf);
		if (!this.buf.hasRemaining()) {
			encoder.complete();
		}
	}

	public void gengerateRandomByte() {
		b = new byte[size];
		len = size;
		for (int i = 0; i < size; i++)
			b[i] = (byte) (RandomUtils.nextInt(random, 26) + 'a');
		buf = ByteBuffer.wrap(b);
		
	}

	public long getContentLength() {
		return this.len;
	}

	public boolean isRepeatable() {
		return true;
	}

	public InputStream getContent() {
		return new ByteArrayInputStream(this.b);
	}
	
	public void close() {
		this.buf.rewind();
	}

}
