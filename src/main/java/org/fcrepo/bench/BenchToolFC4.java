/**
 *
 */

package org.fcrepo.bench;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uncommons.maths.random.XORShiftRNG;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.StmtIterator;

/**
 * Fedora 4 Benchmarking Tool
 * @author frank asseg
 *
 */
public class BenchToolFC4 {

    private static final DecimalFormat FORMATTER = new DecimalFormat("000.00");

    private static int numThreads = 0;

    private static int maxThreads = 15;

    private static byte[] randomData;

    private static long processingTime = 0;

    private static final Logger LOG =
            LoggerFactory.getLogger(BenchToolFC4.class);


    private static int getClusterSize(final String uri) throws IOException {
        final Model model = ModelFactory.createDefaultModel();
        model.read(uri + "/rest");
        final StmtIterator it = model.listStatements(model.createResource(uri + "/rest/") ,model.createProperty("http://fedora.info/definitions/v4/repository#clusterSize"), (RDFNode) null);
        if (!it.hasNext()) {
            return 0;
        }
        return Integer.parseInt(it.next().getObject().asLiteral().getString());

    }

    private static List<String> listObjects(final String uri,final int max) throws IOException {
        final DefaultHttpClient client = new DefaultHttpClient();
        final HttpGet get = new HttpGet(uri);
        final HttpResponse resp = client.execute(get);
        final List<String> pids = new ArrayList<String>();
        final Model m = ModelFactory.createDefaultModel();
        m.read( resp.getEntity().getContent(), null, "TURTLE" );
        final Property hasChild = m.createProperty(
                "http://fedora.info/definitions/v4/repository#", "hasChild");
        final NodeIterator childNodes = m.listObjectsOfProperty(hasChild);
        while ( childNodes.hasNext() ) {
            final RDFNode child = childNodes.next();
            if ( child.toString().startsWith(uri)
                    && child.toString().length() > uri.length() ) {
                pids.add( child.toString().replaceAll(".*" + uri + "/","") );
            }
            if ( pids.size() >= max ) { break; }
        }
        get.releaseConnection();
        return pids;
    }
    private static Model readProperties(final String uri) throws IOException {
        final DefaultHttpClient client = new DefaultHttpClient();
        final HttpGet get = new HttpGet(uri);
        final HttpResponse resp = client.execute(get);
        final Model m = ModelFactory.createDefaultModel();
        m.read( resp.getEntity().getContent(), null, "TURTLE" );
        get.releaseConnection();
        return m;
    }

    public static void main(final String[] args) throws IOException {
        final String uri = args[0];
        final int numObjects = Integer.parseInt(args[1]);
        final long size = Long.parseLong(args[2]);
        maxThreads = Integer.parseInt(args[3]);
        String action = "ingest";
        if ( args.length > 4 ) { action = args[4]; }
        final BenchToolFC4 bench = null;
        if ( action != null && action.equals("delete") ) {
            LOG.info("deleting {} objects", numObjects);
        } else if ( action != null && action.equals("update") ) {
            LOG.info("updating {} objects with datastream size {}", numObjects, size);
            randomData =
                    IOUtils.toByteArray(new BenchToolInputStream(size), size);
        } else if ( action != null && action.equals("read") ) {
            LOG.info("reading {} objects with datastream size {}", numObjects, size);
        } else {
            LOG.info("ingesting {} objects with datastream size {}", numObjects, size);
            randomData =
                    IOUtils.toByteArray(new BenchToolInputStream(size), size);
        }

        FileOutputStream ingestOut = null;
        List<String> pids = null;
        Random rnd = null;
        if ( "java.util.Random".equals(System.getProperty("random.impl")) ) {
            rnd = new Random();
        } else {
            rnd = new XORShiftRNG();
        }
        try {
            final int initialClusterSize = getClusterSize(uri);
            LOG.info("Initial cluster size is {}", initialClusterSize);
            ingestOut = new FileOutputStream("ingest.log");
            final long start = System.currentTimeMillis();
            for (int i = 0; i < numObjects; i++) {
                while (numThreads >= maxThreads) {
                    Thread.sleep(10);
                }
                String pid = "benchfc4-" + start + "-" + (i + 1);
                if ( action != null &&
                        (action.equals("update") || action.equals("delete") || action.equals("read")) ) {
                    if ( pids == null ) {
                        pids = listObjects(uri + "/rest/objects",numObjects);
                    }
                    pid = pids.get( rnd.nextInt(pids.size()) );
                    if ( action.equals("delete") ) { pids.remove(pid); }
                }
                final Thread t = new Thread(new ObjectProcessor(uri, ingestOut, pid, size,
                        action));
                t.start();
                numThreads++;
                final float percent = (float) (i + 1) / (float) numObjects * 100f;
                System.out.print("\r" + FORMATTER.format(percent) + "%");
            }
            while (numThreads > 0) {
                Thread.sleep(100);
            }
            final long duration = System.currentTimeMillis() - start;
            System.out.println(" - " + action + " finished");
            final double avgProcessingTime = processingTime / maxThreads;
            LOG.info(
                    "Average total processing time for {} objects is {} ms per thread",
                    numObjects,
                    processingTime/maxThreads);
            LOG.info("Overall client run time took {} ms", duration);
            final int endClusterSize = getClusterSize(uri);
            if (initialClusterSize != endClusterSize) {
                LOG.warn("Initial cluster size was {} but the cluster had size {} at the end", initialClusterSize, endClusterSize);
            }
            LOG.info("Throughput was {} mb/sec/thread", FORMATTER
                    .format((double) numObjects * (double) size / 1024d /
                    processingTime / maxThreads));
        } catch (final Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(ingestOut);
        }

    }

    private static class ObjectProcessor implements Runnable {

        private final DefaultHttpClient client = new DefaultHttpClient();

        private final URI fedoraUri;

        private final OutputStream ingestOut;

        private final long size;

        private final String pid;

        private final String action;

        public ObjectProcessor( String fedoraUri, final OutputStream out, final String pid,
                final long size, final String action) throws IOException {
            super();
            ingestOut = out;
            if (fedoraUri.charAt(fedoraUri.length() - 1) == '/') {
                fedoraUri = fedoraUri.substring(0, fedoraUri.length() - 1);
            }
            this.fedoraUri = URI.create(fedoraUri);
            this.size = size;
            this.pid = pid;
            this.action = action;
        }

        @Override
        public void run() {
            try {
                if ( action != null && action.equals("delete") ) {
                    this.deleteObject();
                } else if ( action != null && action.equals("update") ) {
                    this.updateObject();
                } else if ( action != null && action.equals("read") ) {
                    this.readObject();
                } else {
                    this.ingestObject();
                }
            } catch (final Exception e) {
                e.printStackTrace();
            }
        }

        private void ingestObject() throws Exception {

            final byte[] postData = rewriteData(BenchToolFC4.randomData, pid);
            final HttpPost post =
                    new HttpPost(fedoraUri.toASCIIString() + "/rest/objects/" +
                            pid + "/DS1/fcr:content");
            post.setHeader("Content-Type", "application/octet-stream");
            post.setEntity(new ByteArrayEntity(postData));
            final long start = System.currentTimeMillis();
            final HttpResponse resp = client.execute(post);
            final String answer = IOUtils.toString(resp.getEntity().getContent());
            post.releaseConnection();
            final long end = System.currentTimeMillis();

            if (resp.getStatusLine().getStatusCode() != 201) {
                LOG.error(answer);
                BenchToolFC4.numThreads--;
                throw new Exception(
                        "Unable to ingest object, fedora returned " +
                                resp.getStatusLine().getStatusCode());
            }

            final long duration = end - start;
            IOUtils.write(duration + "\n",
                    ingestOut);
            BenchToolFC4.numThreads--;
            BenchToolFC4.processingTime += duration;
        }

        private void readObject() throws Exception {

            // read properties
            final String objURI = fedoraUri.toASCIIString() + "/rest/objects/" + pid;
            final Model m = readProperties( objURI );
            final Property created = m.createProperty(
                    "http://fedora.info/definitions/v4/repository#", "created");
            final Resource objRes = m.getResource( objURI );
            final Resource dsRes = m.getResource( objURI + "/DS1" );
            final String objCreated = m.getProperty( objRes, created ).getString();
            final String dsCreated = m.getProperty( dsRes, created ).getString();

            // read datastream content
            final HttpGet get =
                    new HttpGet(fedoraUri.toASCIIString() + "/rest/objects/" +
                            pid + "/DS1/fcr:content");
            final long start = System.currentTimeMillis();
            final HttpResponse resp = client.execute(get);
            final InputStream in = resp.getEntity().getContent();
            final byte[] buf = new byte[8192];
            for ( int read = -1; (read = in.read(buf)) != -1;  ) { }
            get.releaseConnection();
            final long end = System.currentTimeMillis();

            if (resp.getStatusLine().getStatusCode() != 200) {
                BenchToolFC4.numThreads--;
                throw new Exception(
                        "Unable to read object: " + pid + ", fedora returned " +
                                resp.getStatusLine().getStatusCode());
            }
            final long duration = end - start;

            IOUtils.write(duration + "\n",
                    ingestOut);
            BenchToolFC4.numThreads--;
            BenchToolFC4.processingTime += duration;
        }

        private void updateObject() throws Exception {
            final byte[] putData =
                    rewriteData(BenchToolFC4.randomData, pid.concat(String
                            .valueOf(System.currentTimeMillis())));

            final HttpPut put =
                    new HttpPut(fedoraUri.toASCIIString() + "/rest/objects/" +
                            pid + "/DS1/fcr:content");
            put.setHeader("Content-Type", "application/octet-stream");
            put.setEntity(new ByteArrayEntity(putData));
            final long start = System.currentTimeMillis();
            final HttpResponse resp = client.execute(put);
            put.releaseConnection();
            final long end = System.currentTimeMillis();

            if (resp.getStatusLine().getStatusCode() != 204) {
                BenchToolFC4.numThreads--;
                throw new Exception(
                        "Unable to ingest object, fedora returned " +
                                resp.getStatusLine().getStatusCode());
            }

            final long duration = end - start;
            IOUtils.write(duration + "\n",
                    ingestOut);
            BenchToolFC4.numThreads--;
            BenchToolFC4.processingTime += duration;
        }

        private void deleteObject() throws Exception {
            final HttpDelete del = new HttpDelete(fedoraUri.toASCIIString()
                    + "/rest/objects/" + pid );
            final long start = System.currentTimeMillis();
            final HttpResponse resp = client.execute(del);
            del.releaseConnection();
            final long end = System.currentTimeMillis();

            if (resp.getStatusLine().getStatusCode() != 204) {
                BenchToolFC4.numThreads--;
                throw new Exception(
                        "Unable to delete object, fedora returned " +
                                resp.getStatusLine().getStatusCode());
            }

            final long duration = end - start;
            IOUtils.write(duration + "\n",
                    ingestOut);
            BenchToolFC4.numThreads--;
            BenchToolFC4.processingTime += duration;
        }

        private byte[] rewriteData(final byte[] randomData,
                final String pid) throws IOException {
            final byte[] pidBytes = pid.getBytes();

            for (int i = 0; i < pidBytes.length; i++) {
                randomData[i] = pidBytes[i];
            }

            return randomData;
        }
    }
}
