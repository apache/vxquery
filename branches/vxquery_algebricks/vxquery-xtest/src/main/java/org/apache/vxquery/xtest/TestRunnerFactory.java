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
package org.apache.vxquery.xtest;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.vxquery.compiler.CompilerControlBlock;
import org.apache.vxquery.context.RootStaticContextImpl;
import org.apache.vxquery.context.StaticContextImpl;
import org.apache.vxquery.xmlquery.query.XMLQueryCompiler;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.common.controllers.CCConfig;
import edu.uci.ics.hyracks.control.common.controllers.NCConfig;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;

public class TestRunnerFactory {
    private List<ResultReporter> reporters;
    private XTestOptions opts;
    private ClusterControllerService cc;
    private NodeControllerService nc1;
    private NodeControllerService nc2;
    private IHyracksClientConnection hcc;

    public TestRunnerFactory(XTestOptions opts) throws Exception {
        reporters = new ArrayList<ResultReporter>();
        this.opts = opts;

        CCConfig ccConfig = new CCConfig();
        ccConfig.clientNetIpAddress = "127.0.0.1";
        ccConfig.clientNetPort = 39000;
        ccConfig.clusterNetIpAddress = "127.0.0.1";
        ccConfig.clusterNetPort = 39001;
        ccConfig.profileDumpPeriod = 10000;
        File outDir = new File("target/ClusterController");
        outDir.mkdirs();
        File ccRoot = File.createTempFile(TestRunnerFactory.class.getName(), ".data", outDir);
        ccRoot.delete();
        ccRoot.mkdir();
        ccConfig.ccRoot = ccRoot.getAbsolutePath();
        cc = new ClusterControllerService(ccConfig);
        cc.start();

        NCConfig ncConfig1 = new NCConfig();
        ncConfig1.ccHost = "localhost";
        ncConfig1.ccPort = 39001;
        ncConfig1.clusterNetIPAddress = "127.0.0.1";
        ncConfig1.dataIPAddress = "127.0.0.1";
        ncConfig1.nodeId = "nc1";
        nc1 = new NodeControllerService(ncConfig1);
        nc1.start();

        NCConfig ncConfig2 = new NCConfig();
        ncConfig2.ccHost = "localhost";
        ncConfig2.ccPort = 39001;
        ncConfig2.clusterNetIPAddress = "127.0.0.1";
        ncConfig2.dataIPAddress = "127.0.0.1";
        ncConfig2.nodeId = "nc2";
        nc2 = new NodeControllerService(ncConfig2);
        nc2.start();

        hcc = new HyracksConnection(ccConfig.clientNetIpAddress, ccConfig.clientNetPort);
        hcc.createApplication("test", null);
    }

    public void registerReporter(ResultReporter reporter) {
        reporters.add(reporter);
    }

    public Runnable createRunner(final TestCase testCase) {
        return new Runnable() {
            @Override
            public void run() {
                TestCaseResult res = new TestCaseResult(testCase);
                if (opts.verbose) {
                    System.err.println("Starting " + testCase.getXQueryDisplayName());
                }
                long start = System.currentTimeMillis();
                try {
                    XMLQueryCompiler compiler = new XMLQueryCompiler(null);
                    File tempFile = File.createTempFile(testCase.getXQueryFile().getName(), ".tmp");
                    tempFile.deleteOnExit();
                    Reader in = new InputStreamReader(new FileInputStream(testCase.getXQueryFile()), "UTF-8");
                    CompilerControlBlock ccb = new CompilerControlBlock(new StaticContextImpl(
                            RootStaticContextImpl.INSTANCE), new FileSplit[] { new FileSplit("nc1",
                            tempFile.getAbsolutePath()) });
                    compiler.compile(testCase.getXQueryDisplayName(), in, ccb, opts.optimizationLevel);
                    JobSpecification spec = compiler.getModule().getHyracksJobSpecification();
                    spec.setMaxReattempts(0);
                    JobId jobId = hcc.createJob("test", spec, EnumSet.of(JobFlag.PROFILE_RUNTIME));
                    hcc.start(jobId);
                    hcc.waitForCompletion(jobId);
                    res.result = FileUtils.readFileToString(tempFile, "UTF-8").trim();
                } catch (Throwable e) {
                    res.error = e;
                } finally {
                    try {
                        res.compare();
                    } catch (Exception e) {
                        System.err.println("Framework error");
                        e.printStackTrace();
                    }
                    long end = System.currentTimeMillis();
                    res.time = end - start;
                    for (ResultReporter r : reporters) {
                        try {
                            r.reportResult(res);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        };
    }

    public void registerReporters(Collection<ResultReporter> reporters) {
        this.reporters.addAll(reporters);
    }

    public void close() throws Exception {
        nc2.stop();
        nc1.stop();
        cc.stop();
    }
}