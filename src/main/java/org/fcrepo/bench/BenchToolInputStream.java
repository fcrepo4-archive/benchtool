/**
 *
 */
package org.fcrepo.bench;

import java.io.IOException;
import java.io.InputStream;
import java.util.Random;


/**
 * @author frank asseg
 *
 */
public class BenchToolInputStream extends InputStream {

    private final long size;
    private final Random rng;
    private final byte[] slice;

    private long bytesRead;
    private int sliceIdx;

    public BenchToolInputStream(long size, byte[] slice, Random rng) {
        super();
        this.slice = slice;
        this.size = size;
        this.rng = rng;
        this.sliceIdx = rng.nextInt(Math.round(slice.length / 2f));
    }

    /* (non-Javadoc)
     * @see java.io.InputStream#read()
     */
    @Override
    public int read() throws IOException {
        if (this.sliceIdx >= this.slice.length - this.sliceIdx){
            this.sliceIdx = this.rng.nextInt(size - bytesRead > slice.length ? (int) Math.round(this.slice.length / 2f) : (int) (size - bytesRead));
        }
        this.bytesRead++;
        return this.slice[this.sliceIdx++];
    }

    /* (non-Javadoc)
     * @see java.io.InputStream#close()
     */
    @Override
    public void close() throws IOException {
        System.out.println("closing after reading " +  bytesRead + " bytes");
        super.close();
    }
}
