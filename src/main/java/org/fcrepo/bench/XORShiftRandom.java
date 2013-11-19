package org.fcrepo.bench;

import java.util.Random;


/**
 * @author Gregory Jansen see http://www.javamex.com/tutorials/random_numbers/
 *         java_util_random_subclassing.shtml
 */
public class XORShiftRandom extends Random {

    /**
     *
     */
    private static final long serialVersionUID = 7513456985746826662L;
    private long seed = System.nanoTime();

    public XORShiftRandom() {
    }

    protected int next(final int nbits) {
        // N.B. Not thread-safe!
        long x = this.seed;
        x ^= (x << 21);
        x ^= (x >>> 35);
        x ^= (x << 4);
        this.seed = x;
        x &= ((1L << nbits) - 1);
        return (int) x;
    }

}
