package com.intel.cosbench.api.nioengine;

import java.util.Random;

import org.apache.commons.io.input.NullInputStream;
import org.apache.commons.lang.math.RandomUtils;

import com.intel.cosbench.log.LogFactory;
import com.intel.cosbench.log.Logger;


/**
 * This class is to generate random data as input stream for data uploading.
 * 
 * @author ywang19, qzheng7
 * 
 */
public class ProducerRandomInputStream extends NullInputStream {

    private static final int SIZE = 4096; // 4 KB

    private byte[] buffer;

//    private boolean hashCheck = false;
//    private HashUtil util = null;
//    private int hashLen = 0;
//    private byte[] hashBytes;
    private long size = 0;
    private long processed = 0;

    private static Logger logger = LogFactory.getSystemLogger();

    public ProducerRandomInputStream(long size, Random random, boolean isRandom,
            boolean hashCheck) {
        super(size);

//        this.hashCheck = hashCheck;
//        this.util = new HashUtil();
//        this.hashLen = this.util.getHashLen();
//        if (size <= hashLen) {
//            logger.warn("The size is too small to embed checksum, will ignore integrity checking.");
//            this.hashCheck = false;
//            this.util = null;
//            this.hashLen = 0;
//        }
        this.size = size;

        buffer = new byte[SIZE];
        if (isRandom)
            for (int i = 0; i < SIZE; i++)
                buffer[i] = (byte) (RandomUtils.nextInt(random, 26) + 'a');
    }

	@Override
    protected int processByte() {
        throw new UnsupportedOperationException("do not read byte by byte");
    }

    @Override
    protected void processBytes(byte[] bytes, int offset, int length) {

//        if (!hashCheck) {
            do {
                int segment = length > SIZE ? SIZE : length;
                System.arraycopy(buffer, 0, bytes, offset, segment);

                length -= segment;
                offset += segment;
            } while (length > 0); // data copy completed

//        } else {
//            if (length <= hashLen) {
//                System.arraycopy(hashBytes, hashLen - length, bytes, 0, length);
//
//                return;
//            }
//
//            int gap = (int) ((processed + length) - (size - hashLen));
//            if (gap > 0) // partial hash needs append in gap area.
//                length -= gap;
//
//            processed += length;
//            do {
//                int segment = length > SIZE ? SIZE : length;
//                System.arraycopy(buffer, 0, bytes, offset, segment);
//                util.update(buffer, 0, segment);
//
//                length -= segment;
//                offset += segment;
//            } while (length > 0); // data copy completed
//
//            if ((gap <= hashLen) && (gap >= 0)) {
//                // append md5 hash
//                String hashString = util.calculateHash();
//
//                try {
//                    hashBytes = hashString.getBytes("UTF-8");
//                } catch (Exception e) {
//                    e.printStackTrace();
//                    hashBytes = hashString.getBytes();
//                }
//
//                if (gap > 0)
//                    System.arraycopy(hashBytes, 0, bytes, offset, gap);
//            }
//
//        }
    }

}
