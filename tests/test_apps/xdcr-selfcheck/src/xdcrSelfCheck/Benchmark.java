/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package xdcrSelfCheck;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.VoltLogger;
import org.voltdb.CLIConfig;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.utils.MiscUtils;
import xdcrSelfCheck.resolves.ConflictResolveChecker;

public class Benchmark {

    static VoltLogger log = new VoltLogger("Benchmark");

    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------";

    // validated command line configuration
    final Config config;
    // create a primaryClient for each xdcr1 node
    Client primaryClient;
    // create a secondaryClient for each xdcr2 node
    Client secondaryClient;
    // Timer for periodic stats printing
    Timer timer;
    // Benchmark start time
    long benchmarkStartTS;
    // Timer to time the run
    Timer runTimer;
    // Timer for writing the checkpoint count for apprunner
    Timer checkpointTimer;
    // Timer for refreshing ratelimit permits
    Timer permitsTimer;

    final XdcrRateLimiter rateLimiter;

    final ClientPayloadProcessor processor;

    final AtomicInteger activePrimaryConnections = new AtomicInteger(0);
    final AtomicInteger activeSecondaryConnections = new AtomicInteger(0);
    final AtomicBoolean shutdown = new AtomicBoolean(false);

    // for reporting and detecting progress
    public static AtomicLong txnCount = new AtomicLong();
    private long txnCountAtLastCheck;
    private long lastProgressTimestamp = System.currentTimeMillis();

    // For retry connections
    private final ExecutorService es = Executors.newCachedThreadPool(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable arg0) {
            Thread thread = new Thread(arg0, "Retry Connection");
            thread.setDaemon(true);
            return thread;
        }
    });

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    private static class Config extends CLIConfig {
        @Option(desc = "Interval for performance feedback, in seconds.")
        long displayinterval = 5;

        @Option(desc = "Benchmark duration, in seconds.")
        int duration = 20;

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String primaryservers = "localhost:21212";
        String[] parsedPrimaryServers = null;

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String secondaryservers = "localhost:21214";
        String[] parsedSecondaryServers = null;

        @Option(desc = "Number of parallel syncrhonous threads.")
        int threads = 100;

        @Option(desc = "Id of the first thread (useful for running multiple clients).")
        int threadoffset = 0;

        @Option(desc = "Minimum value size in bytes.")
        int minvaluesize = 1024;

        @Option(desc = "Maximum value size in bytes.")
        int maxvaluesize = 1024;

        @Option(desc = "Number of values considered for each value byte.")
        int entropy = 127;

        @Option(desc = "Compress values on the primaryClient side.")
        boolean usecompression = false;

        @Option(desc = "Timeout that kills the primaryClient if progress is not made.")
        int progresstimeout = 120;

        @Option(desc = "Maximum TPS rate for benchmark.")
        int ratelimit = Integer.MAX_VALUE;

        @Option(desc = "Primary cluster voltdbroot")
        String primaryvoltdbroot = "./voltxdcr1";

        @Option(desc = "Secondary cluster voltdbroot")
        String secondaryvoltdbroot = "./voltxdcr2";

        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
            if (threadoffset < 0) exitWithMessageAndUsage("threadoffset must be >= 0");
            if (threads <= 0) exitWithMessageAndUsage("threads must be > 0");
            if (threadoffset > 127) exitWithMessageAndUsage("threadoffset must be within [0, 127]");
            if (threadoffset + threads > 127) exitWithMessageAndUsage("max thread offset must be <= 127");
            if (ratelimit <= 0) exitWithMessageAndUsage("ratelimit must be > 0");

            if (minvaluesize <= 0) exitWithMessageAndUsage("minvaluesize must be > 0");
            if (maxvaluesize <= 0) exitWithMessageAndUsage("maxvaluesize must be > 0");
            if (entropy <= 0) exitWithMessageAndUsage("entropy must be > 0");
            if (entropy > 127) exitWithMessageAndUsage("entropy must be <= 127");
        }

        @Override
        public void parse(String cmdName, String[] args) {
            super.parse(cmdName, args);

            // parse servers
            parsedPrimaryServers = primaryservers.split(",");
            parsedSecondaryServers = secondaryservers.split(",");
        }
    }

    /**
     * Fake an internal jstack to the LOG
     */
    static public void printJStack() {

        Map<String, List<String>> deduped = new HashMap<String, List<String>>();

        // collect all the output, but dedup the identical stack traces
        for (Entry<Thread, StackTraceElement[]> e : Thread.getAllStackTraces().entrySet()) {
            Thread t = e.getKey();
            String header = String.format("\"%s\" %sprio=%d tid=%d %s",
                    t.getName(),
                    t.isDaemon() ? "daemon " : "",
                    t.getPriority(),
                    t.getId(),
                    t.getState().toString());

            String stack = "";
            for (StackTraceElement ste : e.getValue()) {
                stack += "    at " + ste.toString() + "\n";
            }

            if (deduped.containsKey(stack)) {
                deduped.get(stack).add(header);
            }
            else {
                ArrayList<String> headers = new ArrayList<String>();
                headers.add(header);
                deduped.put(stack, headers);
            }
        }

        String logline = "";
        for (Entry<String, List<String>> e : deduped.entrySet()) {
            for (String header : e.getValue()) {
                logline += "\n" + header + "\n";
            }
            logline += e.getKey();
        }
        log.info("Full thread dump:\n" + logline);
    }

    static public void hardStop(String msg) {
        logHardStop(msg);
        stopTheWorld();
    }

    static public void hardStop(Exception e) {
        logHardStop("Unexpected exception", e);
        stopTheWorld();
    }

    static public void hardStop(String msg, Throwable r) {
        logHardStop(msg, r);
        if (r instanceof ProcCallException) {
            ClientResponse cr = ((ProcCallException) r).getClientResponse();
            hardStop(msg, cr);
        }

        if (r instanceof  VoltProcedure.VoltAbortException) {
            stopTheWorld();
        }
    }

    static public void hardStop(String msg, ClientResponse resp) {
        hardStop(msg, (ClientResponseImpl) resp);
    }

    static public void hardStop(String msg, ClientResponseImpl resp) {
        logHardStop(msg);
        log.error("[HardStop] " + resp.getStatusString());
        stopTheWorld();
    }

    static private void logHardStop(String msg, Throwable r) {
        log.error("[HardStop] " + msg, r);
    }

    static private void logHardStop(String msg) {
        log.error("[HardStop] " + msg);
    }

    static private void stopTheWorld() {
        //Benchmark.printJStack();
        log.error("Terminating abnormally");
        System.exit(-1);
    }


    private class PrimaryStatusListener extends ClientStatusListenerExt {

        @Override
        public void uncaughtException(ProcedureCallback callback, ClientResponse resp, Throwable e) {
            hardStop("Uncaught exception in procedure callback ", new Exception(e));

        }

        /**
         * Remove the primaryClient from the list if connection is broken.
         */
        @Override
        public void connectionLost(String hostname, int port, int connectionsLeft, DisconnectCause cause) {
            if (shutdown.get()) {
                return;
            }

            activePrimaryConnections.decrementAndGet();

            // resetTable the connection id so the primaryClient will connect to a recovered cluster
            // this is a bit of a hack
            if (connectionsLeft == 0) {
                ((ClientImpl) primaryClient).resetInstanceId();
            }

            // if the benchmark is still active
            if ((System.currentTimeMillis() - benchmarkStartTS) < (config.duration * 1000)) {
                log.warn(String.format("Connection to %s:%d was lost.", hostname, port));
            }

            // setup for retry
            final String server = MiscUtils.getHostnameColonPortString(hostname, port);
            es.execute(new Runnable() {
                @Override
                public void run() {
                    connectToOneServerWithRetry(primaryClient, server, activePrimaryConnections);
                }
            });
        }
    }

    private class SecondaryStatusListener extends ClientStatusListenerExt {

        @Override
        public void uncaughtException(ProcedureCallback callback, ClientResponse resp, Throwable e) {
            hardStop("Uncaught exception in procedure callback ", new Exception(e));

        }

        /**
         * Remove the primaryClient from the list if connection is broken.
         */
        @Override
        public void connectionLost(String hostname, int port, int connectionsLeft, DisconnectCause cause) {
            if (shutdown.get()) {
                return;
            }

            activeSecondaryConnections.decrementAndGet();

            // resetTable the connection id so the primaryClient will connect to a recovered cluster
            // this is a bit of a hack
            if (connectionsLeft == 0) {
                ((ClientImpl) primaryClient).resetInstanceId();
            }

            // if the benchmark is still active
            if ((System.currentTimeMillis() - benchmarkStartTS) < (config.duration * 1000)) {
                log.warn(String.format("Connection to %s:%d was lost.", hostname, port));
            }

            // setup for retry
            final String server = MiscUtils.getHostnameColonPortString(hostname, port);
            es.execute(new Runnable() {
                @Override
                public void run() {
                    connectToOneServerWithRetry(secondaryClient, server, activeSecondaryConnections);
                }
            });
        }
    }

    /**
     * Constructor for benchmark instance.
     * Configures VoltDB primaryClient and prints configuration.
     *
     * @param config Parsed & validated CLI options.
     */
    Benchmark(Config config) {
        this.config = config;

        rateLimiter = new XdcrRateLimiter(config.ratelimit);
        processor = new ClientPayloadProcessor(4, config.minvaluesize, config.maxvaluesize,
                                         config.entropy, Integer.MAX_VALUE, config.usecompression);

        log.info(HORIZONTAL_RULE);
        log.info(" Command Line Configuration");
        log.info(HORIZONTAL_RULE);
        log.info(config.getConfigDumpString());

        ClientConfig clientConfig = new ClientConfig("", "", new PrimaryStatusListener());
        primaryClient = ClientFactory.createClient(clientConfig);
        clientConfig = new ClientConfig("", "", new SecondaryStatusListener());
        secondaryClient = ClientFactory.createClient(clientConfig);
    }

    /**
     * Connect to a single server with retry. Limited exponential backoff.
     * No timeout. This will run until the process is killed if it's not
     * able to connect.
     *
     * @param server hostname:port or just hostname (hostname can be ip).
     */
    private void connectToOneServerWithRetry(Client client, String server, AtomicInteger activeConnections) {
        int sleep = 1000;
        while (!shutdown.get()) {
            try {
                client.createConnection(server);
                activeConnections.incrementAndGet();
                log.info(String.format("Connected to VoltDB node at: %s.", server));
                break;
            }
            catch (Exception e) {
                log.warn(String.format("Connection to " + server + " failed - retrying in %d second(s).", sleep / 1000));
                try { Thread.sleep(sleep); } catch (Exception interruted) {}
                if (sleep < 8000) sleep += sleep;
            }
        }
    }

    /**
     * Connect to a set of servers in parallel. Each will retry until
     * connection. This call will block until all have connected.
     *
     * @throws InterruptedException if anything bad happens with the threads.
     */
    private void connect() throws InterruptedException {
        log.info("Connecting to VoltDB...");

        final CountDownLatch primaryConnections = new CountDownLatch(1);
        // use a new thread to connect to each server
        for (final String server : config.parsedPrimaryServers) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    connectToOneServerWithRetry(primaryClient, server, activePrimaryConnections);
                    primaryConnections.countDown();
                }
            }).start();
        }
        // block until at least one connection is established
        primaryConnections.await();

        final CountDownLatch secondaryConnections = new CountDownLatch(1);
        // use a new thread to connect to each server
        for (final String server : config.parsedSecondaryServers) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    connectToOneServerWithRetry(secondaryClient, server, activeSecondaryConnections);
                    secondaryConnections.countDown();
                }
            }).start();
        }
        // block until at least one connection is established
        secondaryConnections.await();
    }

    /**
     * Create a Timer task to write the value of the txnCount to
     * disk to make it available to apprunner
     */
    private void schedulePeriodicCheckpoint() throws IOException {
        checkpointTimer = new Timer("Checkpoint Timer", true);
        TimerTask checkpointTask = new TimerTask() {
            @Override
            public void run() {
                String count = String.valueOf(txnCount.get()) + "\n";
                try {
                    FileWriter writer = new FileWriter(".checkpoint", false);
                    writer.write(count);
                    writer.close();
                }
                catch (Exception e) {
                    System.err.println("Caught exception writing checkpoint file.");
               }
            }
        };
        checkpointTimer.scheduleAtFixedRate(checkpointTask, 1 * 1000, 1 * 1000);
    }

    /**
     * Create a Timer task to display performance data on the Vote procedure
     * It calls printStatistics() every displayInterval seconds
     */
    private void schedulePeriodicStats() {
        timer = new Timer("Stats Timer", true);
        TimerTask statsPrinting = new TimerTask() {
            @Override
            public void run() {
                printStatistics();
            }
        };
        timer.scheduleAtFixedRate(statsPrinting,
                                  config.displayinterval * 1000,
                                  config.displayinterval * 1000);
    }

    /**
     * Create a Timer task to refresh ratelimit permits
     */
    private void scheduleRefreshPermits() {
        permitsTimer = new Timer("Ratelimiter Permits Timer", true);
        TimerTask refreshPermits = new TimerTask() {
            @Override
            public void run() { rateLimiter.updateActivePermits(System.currentTimeMillis()); }
        };
        permitsTimer.scheduleAtFixedRate(refreshPermits, 0, 10);
    }

    /**
     * Prints a one line update on performance that can be printed
     * periodically during a benchmark.
     */
    private synchronized void printStatistics() {

        long txnCountNow = txnCount.get();
        long now = System.currentTimeMillis();
        boolean madeProgress = txnCountNow > txnCountAtLastCheck;

        if (madeProgress) {
            lastProgressTimestamp = now;
        }
        txnCountAtLastCheck = txnCountNow;
        long diffInSeconds = (now - lastProgressTimestamp) / 1000;

        log.info(String.format("Executed %d%s", txnCount.get(),
                madeProgress ? "" : " (no progress made in " + diffInSeconds + " seconds, last at " +
                        (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")).format(new Date(lastProgressTimestamp)) + ")"));

        if (diffInSeconds > config.progresstimeout) {
            log.error("No progress was made in over " + diffInSeconds + " seconds while connected to a cluster. Exiting.");
            printJStack();
            System.exit(-1);
        }
    }

    private int getUniquePartitionCount() throws Exception {
        int partitionCount = -1;
        ClientResponse cr = primaryClient.callProcedure("@Statistics", "PARTITIONCOUNT");

        if (cr.getStatus() != ClientResponse.SUCCESS) {
            log.error("Failed to call Statistics proc at startup. Exiting.");
            log.error(((ClientResponseImpl) cr).toJSONString());
            printJStack();
            System.exit(-1);
        }

        VoltTable t = cr.getResults()[0];
        partitionCount = (int) t.fetchRow(0).getLong(3);
        log.info("unique partition count is " + partitionCount);
        if (partitionCount <= 0) {
            log.error("partition count is zero");
            System.exit(-1);
        }
        return partitionCount;
    }

    public static Thread.UncaughtExceptionHandler h = new UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread th, Throwable ex) {
        log.error("Uncaught exception: " + ex.getMessage(), ex);
        printJStack();
        System.exit(-1);
        }
    };

    List<ClientThread> clientThreads = null;

    /**
     * Core benchmark code.
     * Connect. Initialize. Run the loop. Cleanup. Print Results.
     *
     * @throws Exception if anything unexpected happens.
     */
    public void runBenchmark() throws Exception {
        log.info(HORIZONTAL_RULE);
        log.info(" Setup & Initialization");
        log.info(HORIZONTAL_RULE);

        // Only rate limit the ClientThread for now. Share the same permits for all type of invocations.
        Semaphore permits = rateLimiter.addType(0, 1);

        final int cidCount = 128;
        final long[] lastRid = new long[cidCount];
        for (int i = 0; i < lastRid.length; i++) {
            lastRid[i] = 0;
        }

        // connect to one or more servers, loop until success
        connect();

        // get partition count
        int partitionCount = 0;
        int trycount = 12;
        while (trycount-- > 0) {
            try {
                partitionCount = getUniquePartitionCount();
                break;
            } catch (Exception e) {
            }
            Thread.sleep(10000);
        }

        // get stats
        try {
            ClientResponse cr = primaryClient.callProcedure("Summarize", config.threadoffset, config.threads);
            if (cr.getStatus() != ClientResponse.SUCCESS) {
                log.error("Failed to call Summarize proc at startup. Exiting.");
                log.error(((ClientResponseImpl) cr).toJSONString());
                printJStack();
                System.exit(-1);
            }

            // successfully called summarize
            VoltTable t = cr.getResults()[0];
            long ts = t.fetchRow(0).getLong("ts");
            String tsStr = ts == 0 ? "NO TIMESTAMPS" : String.valueOf(ts) + " / " + new Date(ts).toString();
            long count = t.fetchRow(0).getLong("count");

            log.info("STARTUP TIMESTAMP OF LAST UPDATE (GMT): " + tsStr);
            log.info("UPDATES RUN AGAINST THIS DB TO DATE: " + count);
        } catch (ProcCallException e) {
            log.error("Failed to call Summarize proc at startup. Exiting.", e);
            log.error(((ClientResponseImpl) e.getClientResponse()).toJSONString());
            printJStack();
            System.exit(-1);
        }

        CountDownLatch latch = new CountDownLatch(config.threads);
        clientThreads = new ArrayList<>();
        for (byte cid = (byte) config.threadoffset; cid < config.threadoffset + config.threads; cid++) {
            ClientThread clientThread = new ClientThread(cid, txnCount, primaryClient, secondaryClient, processor, permits, latch);
            clientThreads.add(clientThread);
        }

        log.info(HORIZONTAL_RULE);
        log.info("Starting Benchmark");
        log.info(HORIZONTAL_RULE);

        // print periodic statistics to the console
        benchmarkStartTS = System.currentTimeMillis();
        scheduleRunTimer(latch);

        // resetTable progress tracker
        lastProgressTimestamp = System.currentTimeMillis();
        schedulePeriodicStats();
        schedulePeriodicCheckpoint();
        scheduleRefreshPermits();

        // Run the benchmark loop for the requested duration
        // The throughput may be throttled depending on primaryClient configuration
        log.info("Running benchmark...");
        while (((ClientImpl) primaryClient).isHashinatorInitialized() == false) {
            Thread.sleep(1000);
            System.out.println("Wait for hashinator..");
        }

        for (ClientThread t : clientThreads) {
            t.start();
        }

        log.info("All threads started...");
        Thread.currentThread().join();
    }

    void shutdow() {
        log.info(HORIZONTAL_RULE);
        log.info("Benchmark Complete");


        // cancel periodic stats printing
        timer.cancel();
        checkpointTimer.cancel();

        int exitcode = 0;
        long count = txnCount.get();
        log.info("Client thread transaction count: " + count + "\n");
        if (txnCount.get() == 0) {
            System.err.println("Shutting down, but found that no work was done.");
            exitcode = 2;
        }
        System.exit(exitcode);
    }

    /**
     * Create a Timer task to time the run
     * at end of run, check if we actually did anything
     */
    private void scheduleRunTimer(final CountDownLatch latch) throws IOException {
        runTimer = new Timer("Run Timer", true);
        TimerTask runEndTask = new TimerTask() {
            @Override
            public void run() {
                try {
                    for (ClientThread t : clientThreads) {
                        t.shutdown();
                    }

                    latch.await();

                    ConflictResolveChecker checker = new ConflictResolveChecker(primaryClient, secondaryClient,
                            config.primaryvoltdbroot, config.secondaryvoltdbroot);
                    checker.runResolveVerification();
                } catch (Throwable t) {
                    log.error("Error running benchmark", t);
                } finally {
                    shutdow();
                }
            }
        };
        runTimer.schedule(runEndTask, config.duration * 1000);
    }

    /**
     * Main routine creates a benchmark instance and kicks off the run method.
     *
     * @param args Command line arguments.
     * @throws Exception if anything goes wrong.
     * @see {@link Config}
     */
    public static void main(String[] args) throws Exception {
        // create a configuration from the arguments
        Config config = new Config();
        config.parse(Benchmark.class.getName(), args);

        Benchmark benchmark = new Benchmark(config);
        benchmark.runBenchmark();
    }
}