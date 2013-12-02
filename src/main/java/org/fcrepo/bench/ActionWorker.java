
package org.fcrepo.bench;

import java.io.IOException;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.fcrepo.bench.BenchTool.Action;
import org.fcrepo.bench.BenchTool.FedoraVersion;

public class ActionWorker implements Callable<BenchToolResult> {

    private final FedoraRestClient fedora;

    private final long binarySize;

    private final Action action;

    public ActionWorker(Action action, URI fedoraUri, long binarySize,
            FedoraVersion version) {
        super();
        this.binarySize = binarySize;
        this.fedora = FedoraRestClient.createClient(fedoraUri, version);
        this.action = action;
    }

    /*
     * (non-Javadoc)
     * @see java.util.concurrent.Callable#call()
     */
    @Override
    public BenchToolResult call() throws Exception {
        /* check the action and run the appropriate test */
        BenchToolResult result;
        switch (this.action) {
            case INGEST:
                return doIngest();
            case UPDATE:
                return doUpdate();
            case READ:
                return doRead();
            case DELETE:
                return doDelete();
            default:
                throw new IllegalArgumentException("The Action " +
                        action.name() +
                        " is not available in the worker thread");
        }
    }

    private BenchToolResult doDelete() throws IOException {
        String pid = UUID.randomUUID().toString();
        long duration = fedora.delete(pid, binarySize);
        return new BenchToolResult(-1f, duration, binarySize);
    }

    private BenchToolResult doRead() throws IOException {
        String pid = UUID.randomUUID().toString();
        long duration = fedora.retrieve(pid, binarySize);
        float tp = binarySize * 1000f / duration;
        return new BenchToolResult(tp, duration, binarySize);
    }

    private BenchToolResult doUpdate() throws IOException {
        String pid = UUID.randomUUID().toString();
        long duration = fedora.update(pid, binarySize);
        float tp = binarySize * 1000f / duration;
        return new BenchToolResult(tp, duration, binarySize);
    }

    private BenchToolResult doIngest() throws IOException {
        String pid = UUID.randomUUID().toString();
        long duration = fedora.create(pid, binarySize);
        float tp = binarySize * 1000f / duration;
        return new BenchToolResult(tp, duration, binarySize);
    }
}
