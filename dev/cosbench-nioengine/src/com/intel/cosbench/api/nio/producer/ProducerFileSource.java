package com.intel.cosbench.api.nio.producer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import org.apache.http.HttpEntity;
import org.apache.http.annotation.NotThreadSafe;
import org.apache.http.entity.FileEntity;
import org.apache.http.nio.ContentEncoder;
import org.apache.http.nio.ContentEncoderChannel;
import org.apache.http.nio.FileContentEncoder;
import org.apache.http.nio.IOControl;
import org.apache.http.util.Args;

/**
 * One producer source which uses one file to produce outgoing data, so the file should be 
 * accessible in the producing procedure.
 * 
 * @author ywang19
 * 
 */
@NotThreadSafe
public class ProducerFileSource extends ProducerSource<File> {

	private final File file;
	private final RandomAccessFile accessfile;
	private FileChannel fileChannel;
	private long idx = -1;

	public ProducerFileSource(final File file) throws FileNotFoundException {
		super();
		Args.notNull(file, "Source file");
		this.file = file;
		this.accessfile = new RandomAccessFile(file, "r");
	}

	private void closeChannel() throws IOException {
		if (this.fileChannel != null) {
			this.fileChannel.close();
			this.fileChannel = null;
		}
	}

	public synchronized void produceContent(final ContentEncoder encoder,
			final IOControl ioctrl) throws IOException {

		if (this.fileChannel == null) {
			this.fileChannel = this.accessfile.getChannel();
			this.idx = 0;
		}
		long transferred;
		if (encoder instanceof FileContentEncoder) {
			transferred = ((FileContentEncoder) encoder).transfer(
					this.fileChannel, this.idx, Integer.MAX_VALUE);
		} else {
			transferred = this.fileChannel.transferTo(this.idx,
					Integer.MAX_VALUE, new ContentEncoderChannel(encoder));
		}
		if (transferred > 0) {
			this.idx += transferred;
		}

		if (this.idx >= this.fileChannel.size()) {
			encoder.complete();
			closeChannel();
		}
	}

	public boolean isRepeatable() {
		return true;
	}

	public long getContentLength() {
		return this.file.length();
	}

	public boolean isStreaming() {
		return false;
	}

	public InputStream getContent() throws IOException {
		return new FileInputStream(this.file);
	}
	
	public HttpEntity getEntity(){
		return new FileEntity(this.file);
	}

	public synchronized void resetRequest() throws IOException {
		closeChannel();
	}

	public synchronized void close() throws IOException {
		try {
			this.accessfile.close();
		} catch (final IOException ignore) {
		}
	}

	@Override
	public File getSource() {
		return this.file;
	}
}
