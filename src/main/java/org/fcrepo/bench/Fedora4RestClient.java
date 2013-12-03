/**
 *
 */

package org.fcrepo.bench;

import java.io.IOException;
import java.net.URI;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.fcrepo.bench.BenchTool.FedoraVersion;

/**
 * @author frank asseg
 *
 */
public class Fedora4RestClient extends FedoraRestClient {

    public Fedora4RestClient(URI fedoraUri) {
        super(fedoraUri, FedoraVersion.FCREPO4);
    }



    @Override
    protected long createObject(String pid) throws IOException {
        HttpPost post = new HttpPost(this.fedoraUri + "/rest/objects/" + pid);
        long time = System.currentTimeMillis();
        HttpResponse resp = BenchTool.httpClient.execute(post);
        long duration = System.currentTimeMillis() - time;
        post.releaseConnection();
        if (resp.getStatusLine().getStatusCode() != 201) {
            throw new IOException("Unable to create object at /objects/" + pid + "\nFedora returned " + resp.getStatusLine().getStatusCode());
        }
        return duration;
    }

    @Override
    protected long createDatastream(String pid, long size) throws IOException {
        String dsUri =
                this.fedoraUri + "/rest/objects/" + pid + "/ds1/fcr:content";
        HttpPost post = new HttpPost(dsUri);
        post.setEntity(new BenchToolEntity(size, BenchTool.RANDOM_SLICE));
        long start = System.currentTimeMillis();
        HttpResponse resp = BenchTool.httpClient.execute(post);
        long duration = System.currentTimeMillis() - start;
        if (resp.getStatusLine().getStatusCode() != 201) {
            throw new IOException("Unable to create datastream at " + dsUri + "\nFedora returned " + resp.getStatusLine().getStatusCode());
        }
        post.releaseConnection();
        return duration;
    }

    @Override
    protected long updateDatastream(String pid, long size) throws IOException {
        String dsUri =
                this.fedoraUri + "/rest/objects/" + pid + "/ds1/fcr:content";
        HttpPut put = new HttpPut(dsUri);
        put.setEntity(new BenchToolEntity(size, BenchTool.RANDOM_SLICE));
        long start = System.currentTimeMillis();
        HttpResponse resp = BenchTool.httpClient.execute(put);
        long duration = System.currentTimeMillis() - start;
        put.releaseConnection();
        if (resp.getStatusLine().getStatusCode() != 204) {
            throw new IOException("Unable to update datastream at " + dsUri + "\nFedora returned " + resp.getStatusLine().getStatusCode());
        }
        return duration;
    }

    @Override
    protected long retrieveDatastream(String pid) throws IOException {
        String dsUri =
                this.fedoraUri + "/rest/objects/" + pid + "/ds1/fcr:content";
        HttpGet get = new HttpGet(dsUri);
        long start = System.currentTimeMillis();
        HttpResponse resp = BenchTool.httpClient.execute(get);
        long duration = System.currentTimeMillis() - start;
        get.releaseConnection();
        if (resp.getStatusLine().getStatusCode() != 200) {
            throw new IOException("Unable to retrieve datastream from " + dsUri + "\nFedora returned " + resp.getStatusLine().getStatusCode());
        }
        return duration;
    }

    @Override
    protected long deleteObject(String pid) throws IOException {
        HttpDelete delete =
                new HttpDelete(this.fedoraUri + "/rest/objects/" + pid);
        long time = System.currentTimeMillis();
        BenchTool.httpClient.execute(delete);
        long duration = System.currentTimeMillis() - time;
        delete.releaseConnection();
        return duration;
    }

    @Override
    protected long deleteDatastream(String pid) throws IOException {
        String dsUri = this.fedoraUri + "/rest/objects/" + pid + "/ds1";
        HttpDelete delete = new HttpDelete(dsUri);
        long start = System.currentTimeMillis();
        HttpResponse resp = BenchTool.httpClient.execute(delete);
        long duration = System.currentTimeMillis() - start;
        delete.releaseConnection();
        if (resp.getStatusLine().getStatusCode() != 204) {
            throw new IOException("Unable to delete datastream from " + dsUri + "\nFedora returned " + resp.getStatusLine().getStatusCode());
        }
        return duration;
    }
}
