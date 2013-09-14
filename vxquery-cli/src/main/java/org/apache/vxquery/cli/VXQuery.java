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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.vxquery.compiler.CompilerControlBlock;
import org.apache.vxquery.compiler.algebricks.VXQueryGlobalDataFactory;
import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.context.DynamicContextImpl;
import org.apache.vxquery.context.RootStaticContextImpl;
import org.apache.vxquery.context.StaticContextImpl;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.result.ResultUtils;
import org.apache.vxquery.xmlquery.ast.ModuleNode;
import org.apache.vxquery.xmlquery.query.Module;
import org.apache.vxquery.xmlquery.query.XMLQueryCompiler;
import org.apache.vxquery.xmlquery.query.XQueryCompilationListener;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.prettyprint.LogicalOperatorPrettyPrintVisitor;
import edu.uci.ics.hyracks.algebricks.core.algebra.prettyprint.PlanPrettyPrinter;
import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.client.NodeControllerInfo;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.dataset.IHyracksDataset;
import edu.uci.ics.hyracks.api.dataset.IHyracksDatasetReader;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.client.dataset.HyracksDataset;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.common.controllers.CCConfig;
import edu.uci.ics.hyracks.control.common.controllers.NCConfig;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ResultFrameTupleAccessor;

public class VXQuery {
    private final CmdLineOptions opts;

    private ClusterControllerService cc;
    private NodeControllerService[] ncs;
    private IHyracksClientConnection hcc;
    private IHyracksDataset hds;

    private ResultSetId resultSetId;

    public VXQuery(CmdLineOptions opts) {
        this.opts = opts;
    }

    public static void main(String[] args) throws Exception {
        final CmdLineOptions opts = new CmdLineOptions();
        CmdLineParser parser = new CmdLineParser(opts);
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
    }

    private void execute() throws Exception {
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

    private void runQueries() throws IOException, SystemException, Exception {
        for (String query : opts.arguments) {
            String qStr = slurp(query);
            if (opts.showQuery) {
                System.err.println(qStr);
            }
            XQueryCompilationListener listener = new XQueryCompilationListener() {
                @Override
                public void notifyCodegenResult(Module module) {
                    if (opts.showRP) {
                        JobSpecification jobSpec = module.getHyracksJobSpecification();
                        System.err.println(jobSpec.toString());
                    }
                }

                @Override
                public void notifyTranslationResult(Module module) {
                    if (opts.showTET) {
                        try {
                            LogicalOperatorPrettyPrintVisitor v = new LogicalOperatorPrettyPrintVisitor();
                            StringBuilder buffer = new StringBuilder();
                            PlanPrettyPrinter.printPlan(module.getBody(), buffer, v, 0);
                            System.err.println(buffer.toString());
                        } catch (AlgebricksException e) {
                            e.printStackTrace();
                        }
                    }
                }

                @Override
                public void notifyTypecheckResult(Module module) {
                }

                @Override
                public void notifyOptimizedResult(Module module) {
                    if (opts.showOET) {
                        try {
                            LogicalOperatorPrettyPrintVisitor v = new LogicalOperatorPrettyPrintVisitor();
                            StringBuilder buffer = new StringBuilder();
                            PlanPrettyPrinter.printPlan(module.getBody(), buffer, v, 0);
                            System.err.println(buffer.toString());
                        } catch (AlgebricksException e) {
                            e.printStackTrace();
                        }
                    }
                }

                @Override
                public void notifyParseResult(ModuleNode moduleNode) {
                    if (opts.showAST) {
                        System.err.println(new XStream(new DomDriver()).toXML(moduleNode));
                    }
                }
            };

            XMLQueryCompiler compiler = new XMLQueryCompiler(listener, getNodeList(), opts.frameSize);
            resultSetId = createResultSetId();
            CompilerControlBlock ccb = new CompilerControlBlock(new StaticContextImpl(RootStaticContextImpl.INSTANCE),
                    resultSetId);
            compiler.compile(query, new StringReader(qStr), ccb, opts.optimizationLevel);
            if (opts.compileOnly) {
                continue;
            }

            Module module = compiler.getModule();
            JobSpecification js = module.getHyracksJobSpecification();

            DynamicContext dCtx = new DynamicContextImpl(module.getModuleContext());
            js.setGlobalJobDataFactory(new VXQueryGlobalDataFactory(dCtx.createFactory()));

            PrintWriter writer = new PrintWriter(System.out, true);
            for (int i = 0; i < opts.repeatExec; ++i) {
                runJob(js, writer);
            }
        }
    }

    /**
     * Get cluster node configuration.
     */
    private String[] getNodeList() throws Exception {
        Map<String, NodeControllerInfo> nodeControllerInfos = hcc.getNodeControllerInfos();
        String[] nodeList = new String[nodeControllerInfos.size()];
        int index = 0;
        for (String node : nodeControllerInfos.keySet()) {
            nodeList[index++] = node;
        }
        return nodeList;
    }

    private void runJob(JobSpecification spec, PrintWriter writer) throws Exception {
        if (hds == null) {
            hds = new HyracksDataset(hcc, spec.getFrameSize(), 1);
        }

        JobId jobId = hcc.startJob(spec, EnumSet.of(JobFlag.PROFILE_RUNTIME));

        ByteBuffer buffer = ByteBuffer.allocate(spec.getFrameSize());
        IHyracksDatasetReader reader = hds.createReader(jobId, resultSetId);
        IFrameTupleAccessor frameTupleAccessor = new ResultFrameTupleAccessor(spec.getFrameSize());
        buffer.clear();

        while (reader.read(buffer) > 0) {
            buffer.clear();
            writer.print(ResultUtils.getStringFromBuffer(buffer, frameTupleAccessor));
            writer.flush();
        }

        hcc.waitForCompletion(jobId);
    }

    /**
     * Create a unique result set id to get the correct query back from the cluster.
     */
    protected ResultSetId createResultSetId() {
        return new ResultSetId(System.nanoTime());
    }

    public void startLocalHyracks() throws Exception {
        CCConfig ccConfig = new CCConfig();
        ccConfig.clientNetIpAddress = "127.0.0.1";
        ccConfig.clientNetPort = 39000;
        ccConfig.clusterNetIpAddress = "127.0.0.1";
        ccConfig.clusterNetPort = 39001;
        ccConfig.profileDumpPeriod = 10000;
        File outDir = new File("target/ClusterController");
        outDir.mkdirs();
        File ccRoot = File.createTempFile(VXQuery.class.getName(), ".data", outDir);
        ccRoot.delete();
        ccRoot.mkdir();
        ccConfig.ccRoot = ccRoot.getAbsolutePath();
        cc = new ClusterControllerService(ccConfig);
        cc.start();

        ncs = new NodeControllerService[opts.localNodeControllers];
        for (int i = 0; i < ncs.length; i++) {
            NCConfig ncConfig = new NCConfig();
            ncConfig.ccHost = "localhost";
            ncConfig.ccPort = 39001;
            ncConfig.clusterNetIPAddress = "127.0.0.1";
            ncConfig.dataIPAddress = "127.0.0.1";
            ncConfig.datasetIPAddress = "127.0.0.1";
            ncConfig.nodeId = "nc" + (i + 1);
            ncs[i] = new NodeControllerService(ncConfig);
            ncs[i].start();
        }

        hcc = new HyracksConnection(ccConfig.clientNetIpAddress, ccConfig.clientNetPort);
    }

    public void stopLocalHyracks() throws Exception {
        for (int i = 0; i < ncs.length; i++) {
            ncs[i].stop();
        }
        cc.stop();
    }

    private static String slurp(String query) throws IOException {
        return FileUtils.readFileToString(new File(query), "UTF-8");
    }

    private static class CmdLineOptions {
        @Option(name = "-client-net-ip-address", usage = "IP Address of the ClusterController")
        public String clientNetIpAddress = null;

        @Option(name = "-client-net-port", usage = "Port of the ClusterController (default 1098)")
        public int clientNetPort = 1098;

        @Option(name = "-local-node-controllers", usage = "Number of local node controllers (default 1)")
        public int localNodeControllers = 1;

        @Option(name = "-frame-size", usage = "Frame size in bytes. (default 65536)")
        public int frameSize = 65536;

        @Option(name = "-O", usage = "Optimization Level. Default: Full Optimization")
        private int optimizationLevel = Integer.MAX_VALUE;

        @Option(name = "-showquery", usage = "Show query string")
        private boolean showQuery;

        @Option(name = "-showast", usage = "Show abstract syntax tree")
        private boolean showAST;

        @Option(name = "-showtet", usage = "Show translated expression tree")
        private boolean showTET;

        @Option(name = "-showoet", usage = "Show optimized expression tree")
        private boolean showOET;

        @Option(name = "-showrp", usage = "Show Runtime plan")
        private boolean showRP;

        @Option(name = "-compileonly", usage = "Compile the query and stop")
        private boolean compileOnly;

        @Option(name = "-repeatexec", usage = "Number of times to repeat execution")
        private int repeatExec = 1;

        @Option(name = "-timing", usage = "Produce timing information")
        private boolean timing;

        @Option(name = "-x", usage = "Bind an external variable")
        private Map<String, String> bindings = new HashMap<String, String>();

        @Argument
        private List<String> arguments = new ArrayList<String>();
    }

}