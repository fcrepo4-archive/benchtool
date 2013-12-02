
package org.fcrepo.bench;

import java.io.IOException;
import java.net.URI;

import org.fcrepo.bench.BenchTool.FedoraVersion;

public abstract class FedoraRestClient {

    private final FedoraVersion version;

    protected final URI fedoraUri;

    public FedoraRestClient(URI fedoraUri, FedoraVersion version) {
        super();
        this.version = version;
        this.fedoraUri = fedoraUri;
    }

    protected abstract long deleteDatastream(String pid) throws IOException;
    protected abstract long deleteObject(String pid) throws IOException;
    protected abstract long createObject(String pid) throws IOException;
    protected abstract long createDatastream(String pid, long size) throws IOException;
    protected abstract long retrieveDatastream(String pid) throws IOException;
    protected abstract long updateDatastream(String pid, long size) throws IOException;

    final long create(String pid, long size) throws IOException {
        /* first create a new FedoraObject */
        this.createObject(pid);

        /* add a datastream and only measure this period */
        long duration = this.createDatastream(pid, size);

        /* clean up the ds and the parent object */
        this.deleteDatastream(pid);
        this.deleteObject(pid);
        return duration;
    }

    final long update(String pid, long size) throws IOException {
        /*
         * first create an object with a datastream and the measure the update
         * time
         */
        this.createObject(pid);
        this.createDatastream(pid, size);

        /* measure the update */
        long duration = this.updateDatastream(pid, size);

        /* finally delete the parent object */
        this.deleteDatastream(pid);
        this.deleteObject(pid);
        return duration;
    }

    final long retrieve(String pid, long size) throws IOException {
        /*
         * first create an object with a datastream and the measure the
         * retrieval time
         */
        this.createObject(pid);
        this.createDatastream(pid, size);

        long duration = this.retrieveDatastream(pid);

        /* finally delete the parent object */
        this.deleteDatastream(pid);
        this.deleteObject(pid);
        return duration;
    }

    final long delete(String pid, long size) throws IOException {
        /*
         * first create an object with a datastream and the measure the
         * removal time for the datastream
         */
        this.createObject(pid);
        this.createDatastream(pid, size);

        long duration = this.deleteDatastream(pid);

        /* finally delete the parent object */
        this.deleteObject(pid);
        return duration;
    }

    public static FedoraRestClient createClient(URI fedoraUri,
            FedoraVersion version) {
        switch (version) {
            case FCREPO3:
                return new Fedora3RestClient(fedoraUri);
            case FCREPO4:
                return new Fedora4RestClient(fedoraUri);
            default:
                throw new IllegalArgumentException(
                        "No client available for Fedora Version" +
                                version.name());
        }
    }

}
