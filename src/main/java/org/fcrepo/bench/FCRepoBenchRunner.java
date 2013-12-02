/**
 *
 */

package org.fcrepo.bench;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.fcrepo.bench.BenchTool.Action;
import org.fcrepo.bench.BenchTool.FedoraVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.ibm.icu.text.DecimalFormat;

public class FCRepoBenchRunner {

    private static final DecimalFormat FORMAT = new DecimalFormat("###.##");

    private static final Logger LOG = LoggerFactory
            .getLogger(FCRepoBenchRunner.class);

    private final List<BenchToolResult> results = Collections
            .synchronizedList(new ArrayList<BenchToolResult>());

    private final FedoraVersion version;

    private final URI fedoraUri;

    private final Action action;

    private final int numBinaries;

    private final long size;

    private final int numThreads;

    private final ExecutorService executor;

    private FileOutputStream logOut;

    public FCRepoBenchRunner(FedoraVersion version, URI fedoraUri,
            Action action, int numBinaries, long size, int numThreads, String logpath) {
        super();
        this.version = version;
        this.fedoraUri = fedoraUri;
        this.action = action;
        this.numBinaries = numBinaries;
        this.size = size;
        this.numThreads = numThreads;
        this.executor = Executors.newFixedThreadPool(numThreads);
        try {
            this.logOut = new FileOutputStream(logpath);
        } catch (FileNotFoundException e) {
            this.logOut = null;
            LOG.warn("Unable to open log file at {}. No log output will be generated",logpath);
        }
    }

    private int getClusterSize() {
        final Model model = ModelFactory.createDefaultModel();
        model.read(this.fedoraUri + "/rest");
        StmtIterator it =
                model.listStatements(
                        model.createResource(fedoraUri + "/rest/"),
                        model.createProperty("http://fedora.info/definitions/v4/repository#clusterSize"),
                        (RDFNode) null);
        if (!it.hasNext()) {
            return 0;
        }
        return Integer.parseInt(it.next().getObject().asLiteral().getString());
    }

    public void runBenchmark() {
        LOG.info(
                "Running {} {} action(s) against {} with a binary size of {} using {} thread(s)",
                new Object[] {numBinaries, action.name(), version.name(), size,
                        numThreads});
        if (version == FedoraVersion.FCREPO4) {
            LOG.info("The Fedora cluster has {} node(s) before the benchmark",
                    getClusterSize());
        }
        /* schedule all the action workers for exectuion */
        final List<Future<BenchToolResult>> futures = new ArrayList<>();
        for (int i = 0; i < numBinaries; i++) {
            /*
             * schedule the worker thread for execution at some point in the
             * future
             */
            futures.add(executor.submit(new ActionWorker(action, fedoraUri,
                    size, version)));
        }

        /* retrieve the workers' results */
        long duration = 0;
        long numBytes = 0;
        int count = 0;
        float throughputPerThread = 0f;
        for (Future<BenchToolResult> f : futures) {
            try {
                BenchToolResult res = f.get();
                LOG.debug("{} of {} actions finished", ++count, numBinaries);
                duration = duration + res.getDuration();
                numBytes = numBytes + res.getSize();
                if (logOut != null) {
                    logOut.write((res.getDuration() + "\n").getBytes());
                }
                results.add(res);
            } catch (InterruptedException | ExecutionException | IOException e) {
                throw new RuntimeException(e);
            } finally {
                this.executor.shutdown();
            }
        }
        throughputPerThread = numBytes * 1000f / (1024f * 1024f * duration);

        /* now the bench is finished and the result will be printed out */
        LOG.info("Completed {} {} action(s) executed in {} ms", new Object[] {
                this.numBinaries, action, duration});
        if (version == FedoraVersion.FCREPO4) {
            LOG.info("The Fedora cluster has {} node(s) after the benchmark",
                    getClusterSize());
        }
        if (action == Action.UPDATE || action == Action.INGEST ||
                action == Action.READ) {
            if (numThreads == 1) {
                LOG.info("Throughput was {} MB/sec", FORMAT
                        .format(throughputPerThread));
            } else {
                LOG.info("Throughput was {} MB/sec", FORMAT
                        .format(throughputPerThread * numThreads));
                LOG.info("Throughput per thread was {} MB/sec", FORMAT
                        .format(throughputPerThread));
            }
        }
    }
}
