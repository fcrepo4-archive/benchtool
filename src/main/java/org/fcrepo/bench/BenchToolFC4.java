/**
 *
 */

package org.fcrepo.bench;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;

/**
 * @author frank asseg
 *
 */
public class BenchToolFC4 {

    private static final DecimalFormat FORMATTER = new DecimalFormat("000.00");

    private static int numThreads = 0;

    private static int maxThreads = 15;


    public static void main(final String[] args) {
        final String uri = args[0];
        final int numDatastreams = Integer.parseInt(args[1]);
        final int size = Integer.parseInt(args[2]);
        maxThreads = Integer.parseInt(args[3]);
        final BenchToolFC4 bench = null;
        System.out.println("generating " + numDatastreams +
                " datastreams with size " + size);
        FileOutputStream ingestOut = null;
        try {
            ingestOut = new FileOutputStream("ingest.log");
            final long start = System.currentTimeMillis();
            for (int i = 0; i < numDatastreams; i++) {
                while (numThreads >= maxThreads){
                    Thread.sleep(10);
                }
                final Thread t = new Thread(new Ingester(uri, ingestOut, "benchfc4-" + (i+1), size));
                t.start();
                numThreads++;
                final float percent = (float) (i + 1) / (float) numDatastreams * 100f;
                System.out.print("\r" + FORMATTER.format(percent) + "%");
            }
            while(numThreads > 0) {
                Thread.sleep(100);
            }
            final long duration = System.currentTimeMillis() - start;
            System.out.println(" - ingest datastreams finished");
            System.out.println("Complete ingest of " + numDatastreams + " files took " + duration + " ms\n");
            System.out.println("throughput was  " + FORMATTER.format((double) numDatastreams * (double) size /1024d / duration) + " mb/s\n");
        } catch (final Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(ingestOut);
        }

    }

    private static class Ingester implements Runnable{

        private final DefaultHttpClient client = new DefaultHttpClient();

        private final URI fedoraUri;

        private final OutputStream ingestOut;

        private final int size;

        private final String pid;

        public Ingester(String fedoraUri, final OutputStream out, final String pid, final int size) throws IOException {
            super();
            ingestOut = out;
            if (fedoraUri.charAt(fedoraUri.length() - 1) == '/') {
                fedoraUri = fedoraUri.substring(0, fedoraUri.length() - 1);
            }
            this.fedoraUri = URI.create(fedoraUri);
            this.size = size;
            this.pid = pid;
        }

        public void run() {
            try {
                this.ingestObject();
            } catch (final Exception e) {
                e.printStackTrace();
            }
        }

        private void ingestObject() throws Exception {
            final HttpPost post = new HttpPost(fedoraUri.toASCIIString() + "/rest/objects/" + pid + "/DS1/fcr:content");
            post.setHeader("Content-Type", "application/octet-stream");
            post.setEntity(new ByteArrayEntity(getRandomBytes(size)));
            final long start = System.currentTimeMillis();
            final HttpResponse resp = client.execute(post);
            final String answer = IOUtils.toString(resp.getEntity().getContent());
            post.releaseConnection();

            if (resp.getStatusLine().getStatusCode() != 201) {
                System.out.println(answer);
                BenchToolFC4.numThreads--;
                throw new Exception("Unable to ingest object, fedora returned " +
                        resp.getStatusLine().getStatusCode());
            }
            IOUtils.write((System.currentTimeMillis() - start) + "\n", ingestOut);
            BenchToolFC4.numThreads--;
        }

        private byte[] getRandomBytes(final int size) {
            final byte[] data = new byte[size];
            final Random r = new XORShiftRandom();
            r.nextBytes(data);
            return data;
        }
    }
}
