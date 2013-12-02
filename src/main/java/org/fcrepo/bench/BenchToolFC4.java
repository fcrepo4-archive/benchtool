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
import org.apache.http.entity.InputStreamEntity;
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

    private static byte[] randomSlice;

    private final Random rnd;

    static {
        BenchToolFC4.randomSlice = new byte[65535];
        new XORShiftRNG().nextBytes(randomSlice);
    }


    public BenchToolFC4() {
        super();
        if ( "java.util.Random".equals(System.getProperty("random.impl")) ) {
            rnd = new Random();
        } else {
            rnd = new XORShiftRNG();
        }
    }

    private static final Logger LOG =
            LoggerFactory.getLogger(BenchToolFC4.class);


    private static int getClusterSize(String uri) throws IOException {
        final Model model = ModelFactory.createDefaultModel();
        model.read(uri + "/rest");
        StmtIterator it = model.listStatements(model.createResource(uri + "/rest/") ,model.createProperty("http://fedora.info/definitions/v4/repository#clusterSize"), (RDFNode) null);
        if (!it.hasNext()) {
            return 0;
        }
        return Integer.parseInt(it.next().getObject().asLiteral().getString());

    }

    private static List<String> listObjects(String uri,int max) throws IOException {
        DefaultHttpClient client = new DefaultHttpClient();
        HttpGet get = new HttpGet(uri);
        HttpResponse resp = client.execute(get);
        List<String> pids = new ArrayList<String>();
        Model m = ModelFactory.createDefaultModel();
        m.read( resp.getEntity().getContent(), null, "TURTLE" );
        Property hasChild = m.createProperty(
                "http://fedora.info/definitions/v4/repository#", "hasChild");
        NodeIterator childNodes = m.listObjectsOfProperty(hasChild);
        while ( childNodes.hasNext() ) {
            RDFNode child = childNodes.next();
            if ( child.toString().startsWith(uri)
                    && child.toString().length() > uri.length() ) {
                pids.add( child.toString().replaceAll(".*" + uri + "/","") );
            }
            if ( pids.size() >= max ) { break; }
        }
        get.releaseConnection();
        return pids;
    }
    private static Model readProperties(String uri) throws IOException {
        DefaultHttpClient client = new DefaultHttpClient();
        HttpGet get = new HttpGet(uri);
        HttpResponse resp = client.execute(get);
        Model m = ModelFactory.createDefaultModel();
        m.read( resp.getEntity().getContent(), null, "TURTLE" );
        get.releaseConnection();
        return m;
    }

    public static void main(String[] args) {
        String uri = args[0];
        int numObjects = Integer.parseInt(args[1]);
        long size = Long.parseLong(args[2]);
        maxThreads = Integer.parseInt(args[3]);
        String action = "ingest";
        if ( args.length > 4 ) { action = args[4]; }
        BenchToolFC4 bench = null;
        if ( action != null && action.equals("delete") ) {
            LOG.info("deleting {} objects", numObjects);
        } else if ( action != null && action.equals("update") ) {
            LOG.info("updating {} objects with datastream size {}", numObjects, size);
        } else if ( action != null && action.equals("read") ) {
            LOG.info("reading {} objects with datastream size {}", numObjects, size);
        } else {
            LOG.info("ingesting {} objects with datastream size {}", numObjects, size);
        }

        FileOutputStream ingestOut = null;
        List<String> pids = null;
        try {
            final int initialClusterSize = getClusterSize(uri);
            LOG.info("Initial cluster size is {}", initialClusterSize);
            ingestOut = new FileOutputStream("ingest.log");
            long start = System.currentTimeMillis();
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
                Thread t = new Thread(new Ingester(uri, ingestOut, pid, size,
                        action));
                t.start();
                numThreads++;
                float percent = (float) (i + 1) / (float) numObjects * 100f;
                System.out.print("\r" + FORMATTER.format(percent) + "%");
            }
            while (numThreads > 0) {
                Thread.sleep(100);
            }
            long duration = System.currentTimeMillis() - start;
            System.out.println(" - " + action + " finished");
            LOG.info("Processing {} objects took {} ms", numObjects, duration);
            final int endClusterSize = getClusterSize(uri);
            if (initialClusterSize != endClusterSize) {
                LOG.warn("Initial cluster size was {} but the cluster had size {} at the end", initialClusterSize, endClusterSize);
            }
            LOG.info("Throughput was {} mb/s",FORMATTER.format((double) numObjects * (double) size /
                            1024d / duration));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(ingestOut);
        }

    }

    private static class Ingester implements Runnable {

        private final DefaultHttpClient client = new DefaultHttpClient();

        private final URI fedoraUri;

        private final OutputStream ingestOut;

        private final long size;

        private final String pid;

        private final String action;

        public Ingester( String fedoraUri, OutputStream out, String pid,
                long size, String action) throws IOException {
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
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void ingestObject() throws Exception {
            HttpPost post =
                    new HttpPost(fedoraUri.toASCIIString() + "/rest/objects/" +
                            pid + "/DS1/fcr:content");
            post.setHeader("Content-Type", "application/octet-stream");
            post.setEntity(new InputStreamEntity(new BenchToolInputStream(size, randomSlice), size));
            long start = System.currentTimeMillis();
            HttpResponse resp = client.execute(post);
            String answer = IOUtils.toString(resp.getEntity().getContent());
            post.releaseConnection();

            if (resp.getStatusLine().getStatusCode() != 201) {
                LOG.error(answer);
                BenchToolFC4.numThreads--;
                throw new Exception(
                        "Unable to ingest object, fedora returned " +
                                resp.getStatusLine().getStatusCode());
            }
            IOUtils.write((System.currentTimeMillis() - start) + "\n",
                    ingestOut);
            BenchToolFC4.numThreads--;
        }

        private void readObject() throws Exception {
            long start = System.currentTimeMillis();

            // read properties
            String objURI = fedoraUri.toASCIIString() + "/rest/objects/" + pid;
            Model m = readProperties( objURI );
            Property created = m.createProperty(
            "http://fedora.info/definitions/v4/repository#", "created");
            Resource objRes = m.getResource( objURI );
            Resource dsRes = m.getResource( objURI + "/DS1" );
            String objCreated = m.getProperty( objRes, created ).getString();
            String dsCreated = m.getProperty( dsRes, created ).getString();

            // read datastream content
            HttpGet get =
                    new HttpGet(fedoraUri.toASCIIString() + "/rest/objects/" +
                            pid + "/DS1/fcr:content");
            HttpResponse resp = client.execute(get);
            InputStream in = resp.getEntity().getContent();
            byte[] buf = new byte[8192];
            for ( int read = -1; (read = in.read(buf)) != -1;  ) { }
            get.releaseConnection();

            if (resp.getStatusLine().getStatusCode() != 200) {
                BenchToolFC4.numThreads--;
                throw new Exception(
                        "Unable to read object: " + pid + ", fedora returned " +
                                resp.getStatusLine().getStatusCode());
            }
            IOUtils.write((System.currentTimeMillis() - start) + "\n",
                    ingestOut);
            BenchToolFC4.numThreads--;
        }

        private void updateObject() throws Exception {
            HttpPut put =
                    new HttpPut(fedoraUri.toASCIIString() + "/rest/objects/" +
                            pid + "/DS1/fcr:content");
            put.setHeader("Content-Type", "application/octet-stream");
            put.setEntity(new InputStreamEntity(new BenchToolInputStream(size, randomSlice),size));
            long start = System.currentTimeMillis();
            HttpResponse resp = client.execute(put);
            put.releaseConnection();

            if (resp.getStatusLine().getStatusCode() != 204) {
                BenchToolFC4.numThreads--;
                throw new Exception(
                        "Unable to ingest object, fedora returned " +
                                resp.getStatusLine().getStatusCode());
            }
            IOUtils.write((System.currentTimeMillis() - start) + "\n",
                    ingestOut);
            BenchToolFC4.numThreads--;
        }

        private void deleteObject() throws Exception {
            HttpDelete del = new HttpDelete(fedoraUri.toASCIIString()
                + "/rest/objects/" + pid );
            long start = System.currentTimeMillis();
            HttpResponse resp = client.execute(del);
            del.releaseConnection();

            if (resp.getStatusLine().getStatusCode() != 204) {
                BenchToolFC4.numThreads--;
                throw new Exception(
                        "Unable to delete object, fedora returned " +
                                resp.getStatusLine().getStatusCode());
            }
            IOUtils.write((System.currentTimeMillis() - start) + "\n",
                    ingestOut);
            BenchToolFC4.numThreads--;
        }

    }
}
