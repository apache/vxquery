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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.vxquery.compiler.CompilerControlBlock;
import org.apache.vxquery.compiler.algebricks.VXQueryGlobalDataFactory;
import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.context.DynamicContextImpl;
import org.apache.vxquery.context.RootStaticContextImpl;
import org.apache.vxquery.context.StaticContextImpl;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.result.ResultUtils;
import org.apache.vxquery.xmlquery.query.XMLQueryCompiler;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.dataset.IHyracksDataset;
import edu.uci.ics.hyracks.api.dataset.IHyracksDatasetReader;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.client.dataset.HyracksDataset;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.common.controllers.CCConfig;
import edu.uci.ics.hyracks.control.common.controllers.NCConfig;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ResultFrameTupleAccessor;

public class TestRunnerFactory {
    private static final Pattern EMBEDDED_SYSERROR_PATTERN = Pattern
            .compile("org\\.apache\\.vxquery\\.exceptions\\.SystemException: (\\p{javaUpperCase}{4}\\d{4})");

    private List<ResultReporter> reporters;
    private XTestOptions opts;
    private ClusterControllerService cc;
    private NodeControllerService nc1;
    private IHyracksClientConnection hcc;
    private IHyracksDataset hds;

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
        ncConfig1.datasetIPAddress = "127.0.0.1";
        ncConfig1.nodeId = "nc1";
        nc1 = new NodeControllerService(ncConfig1);
        nc1.start();

        hcc = new HyracksConnection(ccConfig.clientNetIpAddress, ccConfig.clientNetPort);
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
                    try {
                        XMLQueryCompiler compiler = new XMLQueryCompiler(null, new String[] { "nc1" });
                        Reader in = new InputStreamReader(new FileInputStream(testCase.getXQueryFile()), "UTF-8");
                        CompilerControlBlock ccb = new CompilerControlBlock(new StaticContextImpl(
                                RootStaticContextImpl.INSTANCE), new ResultSetId(testCase.getXQueryDisplayName()
                                .hashCode()));
                        compiler.compile(testCase.getXQueryDisplayName(), in, ccb, opts.optimizationLevel);
                        JobSpecification spec = compiler.getModule().getHyracksJobSpecification();

                        DynamicContext dCtx = new DynamicContextImpl(compiler.getModule().getModuleContext());
                        spec.setGlobalJobDataFactory(new VXQueryGlobalDataFactory(dCtx.createFactory()));

                        spec.setMaxReattempts(0);
                        JobId jobId = hcc.startJob(spec, EnumSet.of(JobFlag.PROFILE_RUNTIME));

                        if (hds == null) {
                            hds = new HyracksDataset(hcc, spec.getFrameSize(), opts.threads);
                        }
                        ByteBuffer buffer = ByteBuffer.allocate(spec.getFrameSize());
                        IHyracksDatasetReader reader = hds.createReader(jobId, ccb.getResultSetId());
                        IFrameTupleAccessor frameTupleAccessor = new ResultFrameTupleAccessor(spec.getFrameSize());
                        buffer.clear();
                        res.result = "";
                        while (reader.read(buffer) > 0) {
                            buffer.clear();
                            res.result += ResultUtils.getStringFromBuffer(buffer, frameTupleAccessor);
                        }
                        res.result.trim();
                        hcc.waitForCompletion(jobId);
                    } catch (HyracksException e) {
                        Throwable t = e;
                        while (t.getCause() != null) {
                            t = t.getCause();
                        }
                        Matcher m = EMBEDDED_SYSERROR_PATTERN.matcher(t.getMessage());
                        if (m.find()) {
                            String eCode = m.group(1);
                            throw new SystemException(ErrorCode.valueOf(eCode), e);
                        }
                        throw e;
                    }
                } catch (SystemException e) {
                    res.error = e;
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
        nc1.stop();
        cc.stop();
    }
}