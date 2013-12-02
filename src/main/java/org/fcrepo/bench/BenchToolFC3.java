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

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.protocol.ClientContext;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.BasicHttpContext;
import org.uncommons.maths.random.XORShiftRNG;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Fedora 3 Benchmarking Tool
 * @author frank asseg
 *
 */
public class BenchToolFC3 {

    private static final DecimalFormat FORMATTER = new DecimalFormat("000.00");

    private final DefaultHttpClient client = new DefaultHttpClient();

    private final URI fedoraUri;

    private final OutputStream ingestOut;
    private final BasicHttpContext authContext;

    private static byte[] randomData;

    private static long processingTime = 0;

    public BenchToolFC3(String fedoraUri, final String user, final String pass)
            throws IOException {
        super();
        ingestOut = new FileOutputStream("ingest.log");
        if (fedoraUri.charAt(fedoraUri.length() - 1) == '/') {
            fedoraUri = fedoraUri.substring(0, fedoraUri.length() - 1);
        }
        this.fedoraUri = URI.create(fedoraUri);
        this.client.getCredentialsProvider().setCredentials(
                new AuthScope(this.fedoraUri.getHost(), this.fedoraUri
                        .getPort()),
                        new UsernamePasswordCredentials(user, pass));

        // setup authcache to enable pre-emptive auth
        final AuthCache authCache = new BasicAuthCache();
        final BasicScheme basicAuth = new BasicScheme();
        authCache.put(new HttpHost(this.fedoraUri.getHost(),this.fedoraUri.getPort()), basicAuth);
        this.authContext = new BasicHttpContext();
        authContext.setAttribute(ClientContext.AUTH_CACHE, authCache);
    }

    private String ingestObject(final String label) throws Exception {
        final HttpPost post =
                new HttpPost(
                        fedoraUri.toASCIIString() +
                        "/objects/new?format=info:fedora/fedora-system:FOXML-1.1&label=" +
                        label);
        final long start = System.currentTimeMillis();
        final HttpResponse resp = client.execute(post,authContext);
        final String answer = IOUtils.toString(resp.getEntity().getContent());
        post.releaseConnection();
        final long end = System.currentTimeMillis();
        final long duration = end - start;
        processingTime += duration;
        IOUtils.write(duration + "\n", ingestOut);

        if (resp.getStatusLine().getStatusCode() != 201) {
            throw new Exception("Unable to ingest object, fedora returned " +
                    resp.getStatusLine().getStatusCode());
        }

        return answer;
    }

    private void ingestDatastream(final String objectId, final String label, final long size)
            throws Exception {
        final byte[] postData = rewriteData(randomData, objectId.concat(label));
        final HttpPost post = new HttpPost(fedoraUri.toASCIIString() + "/objects/"
                + objectId + "/datastreams/" + label
                + "?versionable=true&controlGroup=M");
        post.setHeader("Content-Type", "application/octet-stream");
        post.setEntity(new ByteArrayEntity(postData));
        final long start = System.currentTimeMillis();
        final HttpResponse resp = client.execute(post, authContext);
        post.releaseConnection();
        final long end = System.currentTimeMillis();
        final long duration = end - start;
        processingTime += duration;
        IOUtils.write(duration + "\n", ingestOut);

        if (resp.getStatusLine().getStatusCode() != 201) {
            throw new Exception("Unable to ingest datastream " + label
                    + " fedora returned " + resp.getStatusLine());
        }
    }

    private void updateDatastream(final String objectId, final String label, final long size)
            throws Exception {
        final byte[] putData =
                rewriteData(randomData, objectId.concat(String.valueOf(System
                        .currentTimeMillis())));
        final HttpPut put = new HttpPut(fedoraUri.toASCIIString() + "/objects/"
                + objectId + "/datastreams/" + label
                + "?versionable=true&controlGroup=M");
        put.setHeader("Content-Type", "application/octet-stream");
        put.setEntity(new ByteArrayEntity(putData));
        final long start = System.currentTimeMillis();
        final HttpResponse resp = client.execute(put, authContext);
        put.releaseConnection();
        final long end = System.currentTimeMillis();
        final long duration = end - start;
        processingTime += duration;
        IOUtils.write(duration + "\n", ingestOut);

        if (resp.getStatusLine().getStatusCode() != 200) {
            throw new Exception("Unable to update datastream " + label
                    + " fedora returned " + resp.getStatusLine());
        }
    }

    private void readDatastream(final String objectId, final String label)
            throws Exception {
        // read properties
        final String objCreated = readProperty(objectId, null, "objCreateDate");
        final String dsCreated = readProperty(objectId, "ds-1", "dsCreateDate");

        // read datastream content
        final HttpGet get = new HttpGet(fedoraUri.toASCIIString() + "/objects/"
                + objectId + "/datastreams/" + label + "/content");
        final long start = System.currentTimeMillis();
        final HttpResponse resp = client.execute(get,authContext);
        final InputStream in = resp.getEntity().getContent();
        final byte[] buf = new byte[8192];
        for ( int read = -1; (read = in.read(buf)) != -1;  ) { }
        get.releaseConnection();
        final long end = System.currentTimeMillis();
        final long duration = end - start;
        processingTime += duration;
        IOUtils.write(duration + "\n", ingestOut);

        if (resp.getStatusLine().getStatusCode() != 200) {
            throw new Exception("Unable to update datastream " + label
                    + " fedora returned " + resp.getStatusLine());
        }
    }

    private void deleteObject(final String objectId) throws Exception {
        final HttpDelete del = new HttpDelete(fedoraUri.toASCIIString() + "/objects/"
                + objectId );
        final long start = System.currentTimeMillis();
        final HttpResponse resp = client.execute(del,authContext);
        del.releaseConnection();
        final long end = System.currentTimeMillis();
        final long duration = end - start;
        processingTime += duration;
        IOUtils.write(duration + "\n", ingestOut);

        if (resp.getStatusLine().getStatusCode() != 200) {
            throw new Exception("Unable to delete object fedora returned "
                    + resp.getStatusLine());
        }
    }

    private List<String> listObjects(final int numObjects) throws Exception {
        final String uri = fedoraUri.toASCIIString() + "/objects?terms=test*"
                + "&resultFormat=xml&pid=true&maxResults="+numObjects;
        final HttpGet get = new HttpGet(uri);
        final HttpResponse resp = client.execute(get,authContext);

        final List<String> pids = new ArrayList<String>();
        final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        final DocumentBuilder builder = factory.newDocumentBuilder();
        final Document document = builder.parse( resp.getEntity().getContent() );
        final NodeList nodeList = document.getElementsByTagName("pid");
        for ( int i = 0; i < nodeList.getLength() && i < numObjects; i++ ) {
            final Node n = nodeList.item(i);
            pids.add( n.getTextContent() );
        }
        get.releaseConnection();
        return pids;
    }
    private String readProperty(final String pid, final String dsName, final String property) throws Exception {
        String uri = fedoraUri.toASCIIString() + "/objects/" + pid;
        if ( dsName != null ) { uri += "/datastreams/" + dsName; }
        uri += "?format=xml";
        final HttpGet get = new HttpGet(uri);
        final HttpResponse resp = client.execute(get,authContext);

        String value = null;
        final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        final DocumentBuilder builder = factory.newDocumentBuilder();
        final Document document = builder.parse( resp.getEntity().getContent() );
        final NodeList nodeList = document.getElementsByTagName(property);
        for ( int i = 0; i < nodeList.getLength() && value == null; i++ ) {
            final Node n = nodeList.item(i);
            value = n.getTextContent();
        }
        get.releaseConnection();
        return value;
    }

    private void shutdown() {
        IOUtils.closeQuietly(ingestOut);
    }

    public static void main(final String[] args) throws IOException {
        final String uri = args[0];
        final String user = args[1];
        final String pass = args[2];
        final int numObjects = Integer.parseInt(args[3]);
        final long size = Integer.parseInt(args[4]);
        String action = "ingest";
        if ( args.length > 5 ) { action = args[5]; }
        BenchToolFC3 bench = null;
        if ( action != null && action.equals("delete") ) {
            System.out.println("deleting " + numObjects + " objects");
        } else if ( action != null && action.equals("update") ) {
            System.out.println("updating " + numObjects
                    + " objects with datastream size " + size);
            randomData =
                    IOUtils.toByteArray(new BenchToolInputStream(size), size);
        } else if ( action != null && action.equals("read") ) {
            System.out.println("reading " + numObjects
                    + " objects with datastream size " + size);
        } else {
            System.out.println("ingesting " + numObjects
                    + " objects with datastream size " + size);
            randomData =
                    IOUtils.toByteArray(new BenchToolInputStream(size), size);
        }
        Random rnd;
        if ( "java.util.Random".equals(System.getProperty("random.impl")) ) {
            rnd = new Random();
        } else {
            rnd = new XORShiftRNG();
        }

        List<String> pids = null;
        try {
            bench = new BenchToolFC3(uri, user, pass);
            final long start = System.currentTimeMillis();
            for (int i = 0; i < numObjects; i++) {
                String objectId = null;
                if ( action != null && (action.equals("delete") || action.equals("update") || action.equals("read")) ) {
                    if ( pids == null ) {
                        pids = bench.listObjects(numObjects);
                    }
                    objectId = pids.get( rnd.nextInt(pids.size()) );
                    if ( action.equals("delete") ) {
                        pids.remove(objectId);
                        bench.deleteObject(objectId);
                    } else if ( action.equals("read") ) {
                        bench.readDatastream(objectId, "ds-1");
                    } else {
                        bench.updateDatastream(objectId, "ds-1", size);
                    }
                } else {
                    objectId = bench.ingestObject("test-" +i);
                    bench.ingestDatastream(objectId, "ds-1", size);
                }
                final float percent = (float) (i + 1) / (float) numObjects * 100f;
                System.out.print("\r" + FORMATTER.format(percent) + "%");
            }
            final long duration = System.currentTimeMillis() - start;
            System.out.println(" - " + action + " finished");
            System.out.println("Total processing time for " + numObjects +
                    " objects is " + processingTime + " ms per thread");
            System.out.println("Overall client run time took " + duration +
                    " ms\n");
            System.out.println("Throughput was  " +
                    FORMATTER.format((double) numObjects * (double) size /
                            1024d / processingTime) + " mb/s\n");

        } catch (final Exception e) {
            e.printStackTrace();
        } finally {
            bench.shutdown();
        }

    }

    private byte[] rewriteData(final byte[] randomData, final String pid)
            throws IOException {
        final byte[] pidBytes = pid.getBytes();

        for (int i = 0; i < pidBytes.length; i++) {
            randomData[i] = pidBytes[i];
        }

        return randomData;
    }
}
