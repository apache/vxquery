/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.vxquery.cli;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.net.InetAddress;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.client.HyracksConnection;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.hyracks.api.dataset.IHyracksDatasetReader;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.client.dataset.HyracksDataset;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.control.nc.resources.memory.FrameManager;
import org.apache.hyracks.dataflow.common.comm.io.ResultFrameTupleAccessor;
import org.apache.vxquery.compiler.CompilerControlBlock;
import org.apache.vxquery.compiler.algebricks.VXQueryGlobalDataFactory;
import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.context.DynamicContextImpl;
import org.apache.vxquery.context.RootStaticContextImpl;
import org.apache.vxquery.context.StaticContextImpl;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.result.ResultUtils;
import org.apache.vxquery.xmlquery.query.Module;
import org.apache.vxquery.xmlquery.query.VXQueryCompilationListener;
import org.apache.vxquery.xmlquery.query.XMLQueryCompiler;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class VXQuery {
    private final CmdLineOptions opts;
    private final CmdLineOptions indexOpts;

    private ClusterControllerService cc;
    private NodeControllerService[] ncs;
    private IHyracksClientConnection hcc;
    private IHyracksDataset hds;
    private List<String> collectionList;
    private ResultSetId resultSetId;
    private static List<String> timingMessages = new ArrayList<>();
    private static long sumTiming;
    private static long sumSquaredTiming;
    private static long minTiming = Long.MAX_VALUE;
    private static long maxTiming = Long.MIN_VALUE;

    /**
     * Constructor to use command line options passed.
     *
     * @param opts
     *            Command line options object
     */
    public VXQuery(CmdLineOptions opts) {
        this.opts = opts;
        // The index query returns only the result, without any other information.
        this.indexOpts = opts;
        indexOpts.showAST = false;
        indexOpts.showOET = false;
        indexOpts.showQuery = false;
        indexOpts.showRP = false;
        indexOpts.showTET = false;
        indexOpts.timing = false;
        indexOpts.compileOnly = false;
        this.collectionList = new ArrayList<String>();
    }

    /**
     * Main method to get command line options and execute query process.
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Date start = new Date();
        final CmdLineOptions opts = new CmdLineOptions();
        CmdLineParser parser = new CmdLineParser(opts);

        // parse command line options, give error message if no arguments passed
        try {
            parser.parseArgument(args);
        } catch (Exception e) {
            parser.printUsage(System.err);
            return;
        }
        if (opts.arguments.isEmpty()) {
            parser.printUsage(System.err);
            return;
        }
        VXQuery vxq = new VXQuery(opts);
        vxq.execute();
        // if -timing argument passed, show the starting and ending times
        if (opts.timing) {
            Date end = new Date();
            timingMessage("Execution time: " + (end.getTime() - start.getTime()) + " ms");
            if (opts.repeatExec > opts.timingIgnoreQueries) {
                Double mean = (double) (sumTiming) / (opts.repeatExec - opts.timingIgnoreQueries);
                double sd = Math.sqrt(sumSquaredTiming / (opts.repeatExec - opts.timingIgnoreQueries) - mean * mean);
                timingMessage("Average execution time: " + mean + " ms");
                timingMessage("Standard deviation: " + String.format("%.4f", sd));
                timingMessage("Coefficient of variation: " + String.format("%.4f", sd / mean));
                timingMessage("Minimum execution time: " + minTiming + " ms");
                timingMessage("Maximum execution time: " + maxTiming + " ms");
            }
            System.out.println("Timing Summary:");
            for (String time : timingMessages) {
                System.out.println("  " + time);
            }
        }

    }

    /**
     * Creates a new Hyracks connection with: the client IP address and port provided, if IP address is provided in command line. Otherwise create a new virtual
     * cluster with Hyracks nodes. Queries passed are run either way. After running queries, if a virtual cluster has been created, it is shut down.
     *
     * @throws Exception
     */
    private void execute() throws Exception {
        System.setProperty("vxquery.buffer_size", Integer.toString(opts.bufferSize));

        if (opts.clientNetIpAddress != null) {
            hcc = new HyracksConnection(opts.clientNetIpAddress, opts.clientNetPort);
            runQueries();
        } else {
            if (!opts.compileOnly) {
                startLocalHyracks();
            }
            try {
                runQueries();
            } finally {
                if (!opts.compileOnly) {
                    stopLocalHyracks();
                }
            }
        }
    }

    /**
     * Reads the contents of the files passed in the list of arguments to a string. If -showquery argument is passed, output the query as string. Run the query
     * for the string.
     *
     * @throws IOException
     * @throws SystemException
     * @throws Exception
     */

    private void runQueries() throws Exception {
        List<String> queries = opts.arguments;
        // Run the showIndexes query before executing any target query, to store the index metadata
        List<String> queriesIndex = new ArrayList<String>();
        queriesIndex.add("vxquery-xtest/src/test/resources/Queries/XQuery/Indexing/Partition-1/showIndexes.xq");
        OutputStream resultStream = new ByteArrayOutputStream();
        executeQuery(queriesIndex.get(0), 1, resultStream, indexOpts);
        ByteArrayOutputStream bos = (ByteArrayOutputStream) resultStream;
        String result = new String(bos.toByteArray());
        String[] collections = result.split("\n");
        this.collectionList = Arrays.asList(collections);
        executeQueries(queries);
    }

    public void executeQueries(List<String> queries) throws Exception {
        for (String query : queries) {
            OutputStream resultStream = System.out;
            if (opts.resultFile != null) {
                resultStream = new FileOutputStream(new File(opts.resultFile));
            }
            executeQuery(query, opts.repeatExec, resultStream, opts);
        }
    }

    public void executeQuery(String query, int repeatedExecution, OutputStream resultStream, CmdLineOptions options)
            throws Exception {
        PrintWriter writer = new PrintWriter(resultStream, true);
        String qStr = slurp(query);
        if (opts.showQuery) {
            writer.println(qStr);
        }
        VXQueryCompilationListener listener = new VXQueryCompilationListener(opts.showAST, opts.showTET, opts.showOET,
                opts.showRP);

        Date start = opts.timing ? new Date() : null;

        Map<String, NodeControllerInfo> nodeControllerInfos = null;
        if (hcc != null) {
            nodeControllerInfos = hcc.getNodeControllerInfos();
        }
        XMLQueryCompiler compiler = new XMLQueryCompiler(listener, nodeControllerInfos, opts.frameSize,
                opts.availableProcessors, opts.joinHashSize, opts.maximumDataSize, opts.hdfsConf);
        resultSetId = createResultSetId();
        CompilerControlBlock ccb = new CompilerControlBlock(new StaticContextImpl(RootStaticContextImpl.INSTANCE),
                resultSetId, null);
        compiler.compile(query, new StringReader(qStr), ccb, opts.optimizationLevel, this.collectionList);
        // if -timing argument passed, show the starting and ending times
        Date end = opts.timing ? new Date() : null;
        if (opts.timing) {
            timingMessage("Compile time: " + (end.getTime() - start.getTime()) + " ms");
        }
        if (opts.compileOnly) {
            return;
        }

        Module module = compiler.getModule();
        JobSpecification js = module.getHyracksJobSpecification();

        DynamicContext dCtx = new DynamicContextImpl(module.getModuleContext());
        js.setGlobalJobDataFactory(new VXQueryGlobalDataFactory(dCtx.createFactory()));

        // Repeat execution for number of times provided in -repeatexec argument
        for (int i = 0; i < repeatedExecution; ++i) {
            start = opts.timing ? new Date() : null;
            runJob(js, writer);
            // if -timing argument passed, show the starting and ending times
            if (opts.timing) {
                end = new Date();
                long currentRun = end.getTime() - start.getTime();
                if ((i + 1) > opts.timingIgnoreQueries) {
                    sumTiming += currentRun;
                    sumSquaredTiming += currentRun * currentRun;
                    if (currentRun < minTiming) {
                        minTiming = currentRun;
                    }
                    if (maxTiming < currentRun) {
                        maxTiming = currentRun;
                    }
                }
                timingMessage("Job (" + (i + 1) + ") execution time: " + currentRun + " ms");
            }
        }
    }

    /**
     * Creates a Hyracks dataset, if not already existing with the job frame size, and 1 reader. Allocates a new buffer of size specified in the frame of Hyracks
     * node. Creates new dataset reader with the current job ID and result set ID. Outputs the string in buffer for each frame.
     *
     * @param spec
     *            JobSpecification object, containing frame size. Current specified job.
     * @param writer
     *            Writer for output of job.
     * @throws Exception
     */
    private void runJob(JobSpecification spec, PrintWriter writer) throws Exception {
        int nReaders = 1;
        if (hds == null) {
            hds = new HyracksDataset(hcc, spec.getFrameSize(), nReaders);
        }

        JobId jobId = hcc.startJob(spec, EnumSet.of(JobFlag.PROFILE_RUNTIME));

        FrameManager resultDisplayFrameMgr = new FrameManager(spec.getFrameSize());
        IFrame frame = new VSizeFrame(resultDisplayFrameMgr);
        IHyracksDatasetReader reader = hds.createReader(jobId, resultSetId);
        IFrameTupleAccessor frameTupleAccessor = new ResultFrameTupleAccessor();

        while (reader.read(frame) > 0) {
            writer.print(ResultUtils.getStringFromBuffer(frame.getBuffer(), frameTupleAccessor));
            writer.flush();
            frame.getBuffer().clear();
        }

        hcc.waitForCompletion(jobId);
    }

    /**
     * Create a unique result set id to get the correct query back from the cluster.
     *
     * @return Result Set id generated with current system time.
     */
    protected ResultSetId createResultSetId() {
        return new ResultSetId(System.nanoTime());
    }

    /**
     * Start local virtual cluster with cluster controller node and node controller nodes. IP address provided for node controller is localhost. Unassigned ports
     * 39000 and 39001 are used for client and cluster port respectively. Creates a new Hyracks connection with the IP address and client ports.
     *
     * @throws Exception
     */
    public void startLocalHyracks() throws Exception {
        String localAddress = InetAddress.getLocalHost().getHostAddress();
        CCConfig ccConfig = new CCConfig();
        ccConfig.clientNetIpAddress = localAddress;
        ccConfig.clientNetPort = 39000;
        ccConfig.clusterNetIpAddress = localAddress;
        ccConfig.clusterNetPort = 39001;
        ccConfig.httpPort = 39002;
        ccConfig.profileDumpPeriod = 10000;
        cc = new ClusterControllerService(ccConfig);
        cc.start();

        ncs = new NodeControllerService[opts.localNodeControllers];
        for (int i = 0; i < ncs.length; i++) {
            NCConfig ncConfig = new NCConfig();
            ncConfig.ccHost = "localhost";
            ncConfig.ccPort = 39001;
            ncConfig.clusterNetIPAddress = localAddress;
            ncConfig.dataIPAddress = localAddress;
            ncConfig.resultIPAddress = localAddress;
            ncConfig.nodeId = "nc" + (i + 1);
            //TODO: enable index folder as a cli option for on-the-fly indexing queries
            ncConfig.ioDevices = Files.createTempDirectory(ncConfig.nodeId).toString();
            ncs[i] = new NodeControllerService(ncConfig);
            ncs[i].start();
        }

        hcc = new HyracksConnection(ccConfig.clientNetIpAddress, ccConfig.clientNetPort);
    }

    /**
     * Shuts down the virtual cluster, along with all nodes and node execution, network and queue managers.
     *
     * @throws Exception
     */
    public void stopLocalHyracks() throws Exception {
        for (int i = 0; i < ncs.length; i++) {
            ncs[i].stop();
        }
        cc.stop();
    }

    /**
     * Reads the contents of file given in query into a String. The file is always closed. For XML files UTF-8 encoding is used.
     *
     * @param query
     *            The query with filename to be processed
     * @return UTF-8 formatted query string
     * @throws IOException
     */
    private static String slurp(String query) throws IOException {
        return FileUtils.readFileToString(new File(query), "UTF-8");
    }

    /**
     * Save and print out the timing message.
     *
     * @param message
     */
    private static void timingMessage(String message) {
        System.out.println(message);
        timingMessages.add(message);
    }

    /**
     * Helper class with fields and methods to handle all command line options
     */
    private static class CmdLineOptions {
        @Option(name = "-available-processors", usage = "Number of available processors. (default: java's available processors)")
        private int availableProcessors = -1;

        @Option(name = "-client-net-ip-address", usage = "IP Address of the ClusterController.")
        private String clientNetIpAddress = null;

        @Option(name = "-client-net-port", usage = "Port of the ClusterController. (default: 1098)")
        private int clientNetPort = 1098;

        @Option(name = "-local-node-controllers", usage = "Number of local node controllers. (default: 1)")
        private int localNodeControllers = 1;

        @Option(name = "-frame-size", usage = "Frame size in bytes. (default: 65,536)")
        private int frameSize = 65536;

        @Option(name = "-join-hash-size", usage = "Join hash size in bytes. (default: 67,108,864)")
        private long joinHashSize = -1;

        @Option(name = "-maximum-data-size", usage = "Maximum possible data size in bytes. (default: 150,323,855,000)")
        private long maximumDataSize = -1;

        @Option(name = "-buffer-size", usage = "Disk read buffer size in bytes.")
        private int bufferSize = -1;

        @Option(name = "-O", usage = "Optimization Level. (default: Full Optimization)")
        private int optimizationLevel = Integer.MAX_VALUE;

        @Option(name = "-showquery", usage = "Show query string.")
        private boolean showQuery;

        @Option(name = "-showast", usage = "Show abstract syntax tree.")
        private boolean showAST;

        @Option(name = "-showtet", usage = "Show translated expression tree.")
        private boolean showTET;

        @Option(name = "-showoet", usage = "Show optimized expression tree.")
        private boolean showOET;

        @Option(name = "-showrp", usage = "Show Runtime plan.")
        private boolean showRP;

        @Option(name = "-compileonly", usage = "Compile the query and stop.")
        private boolean compileOnly;

        @Option(name = "-repeatexec", usage = "Number of times to repeat execution.")
        private int repeatExec = 1;

        @Option(name = "-result-file", usage = "File path to save the query result.")
        private String resultFile = null;

        @Option(name = "-timing", usage = "Produce timing information.")
        private boolean timing;

        @Option(name = "-timing-ignore-queries", usage = "Ignore the first X number of quereies.")
        private int timingIgnoreQueries = 2;

        @Option(name = "-x", usage = "Bind an external variable")
        private Map<String, String> bindings = new HashMap<>();

        @Option(name = "-hdfs-conf", usage = "Directory path to Hadoop configuration files")
        private String hdfsConf = null;

        @Argument
        private List<String> arguments = new ArrayList<>();
    }

}
