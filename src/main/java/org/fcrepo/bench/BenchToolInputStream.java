/**
 *
 */
package org.fcrepo.bench;

import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;

import org.uncommons.maths.random.XORShiftRNG;


/**
 * @author frank asseg
 *
 */
public class BenchToolInputStream extends InputStream {

    private final long size;
    private long idx = 0;
    /* quite the fast RNG from uncommons-math */
    private final XORShiftRNG rng = new XORShiftRNG();
    private static final DecimalFormat FORMATTER = new DecimalFormat("000.00");

    public BenchToolInputStream(long size) {
        super();
        this.size = size;
        System.out.println();
    }

    /* (non-Javadoc)
     * @see java.io.InputStream#read()
     */
    @Override
    public int read() throws IOException {
        if ((idx++) % 67108864 == 0){
            System.out.print("\r" + FORMATTER.format(idx * 100d / size) +  " %");
        }
        return rng.nextInt();
    }
}