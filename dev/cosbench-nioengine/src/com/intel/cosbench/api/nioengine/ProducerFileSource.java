package com.intel.cosbench.api.nioengine;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;

import org.apache.http.annotation.NotThreadSafe;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.ContentEncoder;
import org.apache.http.nio.ContentEncoderChannel;
import org.apache.http.nio.FileContentEncoder;
import org.apache.http.nio.IOControl;
import org.apache.http.util.Args;

/**
 * One consumer sink which uses one file to consume incoming data, so it means one external file will be created to store the data.
 * 
 * @author ywang19
 *
 */
@NotThreadSafe
public class ProducerFileSource extends ProducerSource<File> {

	private final File file;
	private FileChannel fileChannel;
	private long idx = -1;
	private boolean useFileChannels;

	public ProducerFileSource(final File file, final ContentType contentType,
			final boolean useFileChannels) {
		super(file);
		Args.notNull(file, "File");
		this.file = file;
		this.useFileChannels = useFileChannels;
	}

	public ProducerFileSource(final File file) {
		super(file);
		Args.notNull(file, "File");
		this.file = file;
	}

	public ProducerFileSource(final File file, final ContentType contentType) {
		this(file, contentType, true);
	}


	public void close() throws IOException {
		final FileChannel local = fileChannel;
		fileChannel = null;
		if (local != null) {
			local.close();
		}
	}

	public long getContentLength() {
		return file.length();
	}

	public boolean isRepeatable() {
		return true;
	}

	public void produceContent(final ContentEncoder encoder,
			final IOControl ioctrl) throws IOException {
		if (fileChannel == null) {
			fileChannel = new FileInputStream(file).getChannel();
			idx = 0;
		}

		long transferred;
		if (useFileChannels && encoder instanceof FileContentEncoder) {
			transferred = ((FileContentEncoder) encoder).transfer(fileChannel,
					idx, Long.MAX_VALUE);
		} else {
			transferred = fileChannel.transferTo(idx, Long.MAX_VALUE,
					new ContentEncoderChannel(encoder));
		}
		if (transferred > 0) {
			idx += transferred;
		}
		if (idx >= fileChannel.size()) {
			encoder.complete();
			close();
		}
	}

	public boolean isStreaming() {
		return false;
	}

	public InputStream getContent() throws IOException {
		return new FileInputStream(this.file);
	}
}
